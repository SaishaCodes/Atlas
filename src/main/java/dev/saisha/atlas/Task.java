package dev.saisha.atlas;

import java.util.List;
import java.util.Objects;

public record Task(
        String id,
        String name,
        List<String> dependencies,
        int priority,
        int maxAttempts,
        TaskAction action
) {
    public Task {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(dependencies, "dependencies");
        Objects.requireNonNull(action, "action");
        dependencies = List.copyOf(dependencies);
        if (id.isBlank() || name.isBlank()) throw new IllegalArgumentException("id/name cannot be blank");
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
    }
}
