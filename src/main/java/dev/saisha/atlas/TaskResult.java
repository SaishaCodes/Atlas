package dev.saisha.atlas;

public record TaskResult(TaskStatus status, int attempts, String error) {}
