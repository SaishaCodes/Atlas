package dev.saisha.atlas;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) {
        AtomicInteger flakyAttempts = new AtomicInteger();

        try (WorkflowEngine engine = new WorkflowEngine(3, Duration.ofMillis(100))) {
            List<Task> workflow = List.of(
                    new Task("fetch", "Fetch source data", List.of(), 10, 2,
                            () -> System.out.println("Fetching source data")),
                    new Task("transform", "Transform data", List.of("fetch"), 5, 3, () -> {
                        int attempt = flakyAttempts.incrementAndGet();
                        if (attempt < 2) throw new IllegalStateException("Temporary transform failure");
                        System.out.println("Transforming data on attempt " + attempt);
                    }),
                    new Task("publish", "Publish result", List.of("transform"), 1, 2,
                            () -> System.out.println("Publishing result"))
            );

            System.out.println("Workflow results:");
            engine.run(workflow).forEach((id, result) ->
                    System.out.printf("%s -> %s (attempts=%d, error=%s)%n",
                            id, result.status(), result.attempts(), result.error()));
            System.out.println("Dead-letter queue: " + engine.deadLetterQueue());
        }
    }
}
