package dev.saisha.atlas;

@FunctionalInterface
public interface TaskAction {
    void execute() throws Exception;
}
