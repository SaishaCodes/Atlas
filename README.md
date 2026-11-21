# Atlas — Concurrent DAG Workflow Engine

Atlas is a Java 17 reference implementation of a small workflow engine. It executes
DAG-structured tasks using a bounded worker pool and supports dependency resolution,
priority-aware scheduling of ready tasks, retries with exponential backoff, per-engine
idempotency, and a dead-letter queue for exhausted tasks.

## Features

- DAG validation (unknown dependencies, duplicate IDs, self-dependencies, cycles).
- Concurrent execution using a bounded `ExecutorService`.
- A task becomes eligible only after all dependencies finish successfully.
- Retry policies with exponential backoff.
- Failed tasks are recorded in a dead-letter queue after their retry budget is exhausted.
- Task results include status, attempt count, and error information.
- JUnit tests for retry, dead-lettering, and cycle detection.

## Requirements

- JDK 17+
- Maven 3.8+

## Run

```bash
mvn test
mvn compile exec:java
```

## Example

`Main.java` defines `fetch -> transform -> publish`. The transform task intentionally
fails once, then succeeds on retry. Run the program to see task results.

## Design notes and limitations

This implementation is intentionally in-process and single-JVM. The idempotency map,
task results, and dead-letter queue are held in memory; they do not survive restarts and
are not shared across multiple application instances. Priority determines the ordering
of simultaneously-ready tasks during DAG planning; actual execution timing is subject to
the worker pool and dependency completion.

It does **not** claim distributed exactly-once execution, durable scheduling, Redis-backed
coordination, or PostgreSQL persistence. Those are natural next steps for a production
version: persist workflow/task state, use a durable queue, implement leases/heartbeats,
and define retry/dead-letter recovery APIs.

## Possible extensions

1. Add a REST API for workflow submission and status queries.
2. Persist workflow and task state in PostgreSQL.
3. Use Redis or a message broker for distributed worker coordination.
4. Add cancellation, timeouts, metrics, tracing, and graceful recovery after restart.
5. Add integration tests using Testcontainers.

## Repository structure

```text
src/
  main/java/dev/saisha/atlas/
    Main.java
    Task.java
    TaskAction.java
    TaskResult.java
    TaskStatus.java
    WorkflowEngine.java
  test/java/dev/saisha/atlas/
    WorkflowEngineTest.java
```

## License

MIT — see [LICENSE](LICENSE).
