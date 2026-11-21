package dev.saisha.atlas;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class WorkflowEngineTest {
    @Test
    void runsTasksAfterDependenciesAndRetriesFailures() {
        AtomicInteger attempts = new AtomicInteger();
        try (WorkflowEngine engine = new WorkflowEngine(2, Duration.ZERO)) {
            List<Task> tasks = List.of(
                    new Task("a", "A", List.of(), 1, 1, () -> {}),
                    new Task("b", "B", List.of("a"), 1, 2, () -> {
                        if (attempts.incrementAndGet() == 1) throw new RuntimeException("retry me");
                    })
            );
            var results = engine.run(tasks);
            assertEquals(TaskStatus.SUCCEEDED, results.get("a").status());
            assertEquals(TaskStatus.SUCCEEDED, results.get("b").status());
            assertEquals(2, results.get("b").attempts());
        }
    }

    @Test
    void sendsExhaustedTaskToDeadLetterQueue() {
        try (WorkflowEngine engine = new WorkflowEngine(1, Duration.ZERO)) {
            var results = engine.run(List.of(
                    new Task("bad", "Always fails", List.of(), 1, 2,
                            () -> { throw new IllegalStateException("broken"); })
            ));
            assertEquals(TaskStatus.DEAD_LETTERED, results.get("bad").status());
            assertEquals(List.of("bad"), engine.deadLetterQueue());
        }
    }

    @Test
    void rejectsCycles() {
        try (WorkflowEngine engine = new WorkflowEngine(1, Duration.ZERO)) {
            List<Task> tasks = List.of(
                    new Task("a", "A", List.of("b"), 1, 1, () -> {}),
                    new Task("b", "B", List.of("a"), 1, 1, () -> {})
            );
            assertThrows(IllegalArgumentException.class, () -> engine.run(tasks));
        }
    }
}
