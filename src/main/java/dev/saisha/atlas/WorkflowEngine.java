package dev.saisha.atlas;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Small in-process DAG workflow engine.
 *
 * Guarantees within one JVM:
 * - dependencies complete successfully before a task starts;
 * - tasks are submitted to a bounded worker pool;
 * - failed tasks are retried with exponential backoff;
 * - an idempotency key is executed at most once per engine instance;
 * - exhausted tasks are recorded in the dead-letter list.
 *
 * This is an educational reference implementation, not a distributed,
 * multi-node scheduler. See README for production-hardening work.
 */
public final class WorkflowEngine implements AutoCloseable {
    private final ExecutorService workers;
    private final Duration initialBackoff;
    private final ConcurrentMap<String, CompletableFuture<TaskResult>> executions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TaskResult> results = new ConcurrentHashMap<>();
    private final Queue<String> deadLetterQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WorkflowEngine(int workerCount, Duration initialBackoff) {
        if (workerCount < 1) throw new IllegalArgumentException("workerCount must be >= 1");
        this.workers = Executors.newFixedThreadPool(workerCount);
        this.initialBackoff = Objects.requireNonNull(initialBackoff);
    }

    public Map<String, TaskResult> run(List<Task> tasks) {
        if (closed.get()) throw new IllegalStateException("Engine is closed");
        Map<String, Task> graph = validateAndIndex(tasks);
        Map<String, CompletableFuture<TaskResult>> futures = new HashMap<>();

        // Build futures in topological order so every dependency future exists.
        for (String taskId : topologicalOrder(graph)) {
            Task task = graph.get(taskId);
            CompletableFuture<?>[] dependencies = task.dependencies().stream()
                    .map(futures::get).toArray(CompletableFuture[]::new);

            CompletableFuture<TaskResult> future = CompletableFuture
                    .allOf(dependencies)
                    .thenComposeAsync(ignored -> {
                        boolean dependencyFailed = task.dependencies().stream()
                                .map(futures::get)
                                .map(CompletableFuture::join)
                                .anyMatch(r -> r.status() != TaskStatus.SUCCEEDED);
                        if (dependencyFailed) {
                            TaskResult skipped = new TaskResult(TaskStatus.FAILED, 0,
                                    "A dependency did not succeed");
                            results.put(task.id(), skipped);
                            return CompletableFuture.completedFuture(skipped);
                        }
                        return executeIdempotently(task);
                    }, workers);
            futures.put(taskId, future);
        }

        CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new)).join();
        Map<String, TaskResult> snapshot = new LinkedHashMap<>();
        topologicalOrder(graph).forEach(id -> snapshot.put(id, results.get(id)));
        return Collections.unmodifiableMap(snapshot);
    }

    private CompletableFuture<TaskResult> executeIdempotently(Task task) {
        // A task ID is the idempotency key for this engine instance.
        return executions.computeIfAbsent(task.id(), ignored ->
                CompletableFuture.supplyAsync(() -> executeWithRetry(task), workers));
    }

    private TaskResult executeWithRetry(Task task) {
        for (int attempt = 1; attempt <= task.maxAttempts(); attempt++) {
            try {
                task.action().execute();
                TaskResult success = new TaskResult(TaskStatus.SUCCEEDED, attempt, null);
                results.put(task.id(), success);
                return success;
            } catch (Exception exception) {
                if (attempt == task.maxAttempts()) {
                    TaskResult failure = new TaskResult(TaskStatus.DEAD_LETTERED, attempt,
                            exception.getClass().getSimpleName() + ": " + exception.getMessage());
                    results.put(task.id(), failure);
                    deadLetterQueue.add(task.id());
                    return failure;
                }
                sleep(backoffFor(attempt));
            }
        }
        throw new IllegalStateException("Unreachable retry state");
    }

    private Duration backoffFor(int failedAttempt) {
        long multiplier = 1L << Math.min(failedAttempt - 1, 20);
        try {
            return initialBackoff.multipliedBy(multiplier);
        } catch (ArithmeticException overflow) {
            return Duration.ofSeconds(30);
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CompletionException(interrupted);
        }
    }

    private static Map<String, Task> validateAndIndex(List<Task> tasks) {
        Map<String, Task> graph = new LinkedHashMap<>();
        for (Task task : tasks) {
            if (graph.putIfAbsent(task.id(), task) != null) {
                throw new IllegalArgumentException("Duplicate task ID: " + task.id());
            }
        }
        for (Task task : tasks) {
            for (String dependency : task.dependencies()) {
                if (!graph.containsKey(dependency)) {
                    throw new IllegalArgumentException("Unknown dependency " + dependency + " for " + task.id());
                }
                if (dependency.equals(task.id())) {
                    throw new IllegalArgumentException("Task cannot depend on itself: " + task.id());
                }
            }
        }
        return graph;
    }

    private static List<String> topologicalOrder(Map<String, Task> graph) {
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();
        graph.keySet().forEach(id -> {
            indegree.put(id, 0);
            dependents.put(id, new ArrayList<>());
        });
        graph.values().forEach(task -> {
            indegree.put(task.id(), task.dependencies().size());
            task.dependencies().forEach(dep -> dependents.get(dep).add(task.id()));
        });

        PriorityQueue<String> ready = new PriorityQueue<>(
                Comparator.<String>comparingInt(id -> graph.get(id).priority()).reversed()
                        .thenComparing(id -> id));
        indegree.forEach((id, degree) -> { if (degree == 0) ready.add(id); });

        List<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            String id = ready.poll();
            order.add(id);
            for (String child : dependents.get(id)) {
                int next = indegree.merge(child, -1, Integer::sum);
                if (next == 0) ready.add(child);
            }
        }
        if (order.size() != graph.size()) throw new IllegalArgumentException("Workflow contains a dependency cycle");
        return order;
    }

    public List<String> deadLetterQueue() {
        return List.copyOf(deadLetterQueue);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            workers.shutdown();
            try {
                if (!workers.awaitTermination(5, TimeUnit.SECONDS)) workers.shutdownNow();
            } catch (InterruptedException interrupted) {
                workers.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
