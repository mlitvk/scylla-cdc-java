package com.scylladb.cdc.lib;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.master.GenerationMetadata;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.Worker;
import com.scylladb.cdc.model.worker.WorkerConfiguration;
import com.scylladb.cdc.transport.MasterTransport;
import com.scylladb.cdc.transport.GroupedTasks;
import com.scylladb.cdc.transport.WorkerTransport;

class LocalTransport implements MasterTransport, WorkerTransport {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final ThreadGroup workersThreadGroup;
    private final WorkerConfiguration.Builder workerConfigurationBuilder;
    private final ConcurrentHashMap<TaskId, TaskState> taskStates = new ConcurrentHashMap<>();
    private final Supplier<ScheduledExecutorService> executorServiceSupplier;
    private Optional<GenerationId> currentGenerationId = Optional.empty();

    // Single worker reference
    private Worker currentWorker = null;
    private Thread workerThread = null;

    // Helper class to store generation metadata and latest consumed timestamp
    protected static class TableGenerationState {
        public GenerationMetadata generationMetadata;
        public Optional<Timestamp> maxConsumedTimestamp = Optional.empty();

        public TableGenerationState(GenerationMetadata generationMetadata) {
            this.generationMetadata = generationMetadata;
        }
    }

    // Track generation state by table for tablet mode
    protected final Map<TableName, TableGenerationState> currentGenerationByTable = new ConcurrentHashMap<>();

    public LocalTransport(ThreadGroup cdcThreadGroup, WorkerConfiguration.Builder workerConfigurationBuilder,
                          Supplier<ScheduledExecutorService> executorServiceSupplier) {
        workersThreadGroup = new ThreadGroup(cdcThreadGroup, "Scylla-CDC-Worker-Threads");
        this.workerConfigurationBuilder = Preconditions.checkNotNull(workerConfigurationBuilder);
        this.executorServiceSupplier = Preconditions.checkNotNull(executorServiceSupplier);
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId() {
        return currentGenerationId;
    }

    @Override
    public Optional<GenerationId> getCurrentGenerationId(TableName tableName) {
        TableGenerationState state = currentGenerationByTable.get(tableName);
        if (state == null) {
            return Optional.empty();
        }
        return Optional.of(state.generationMetadata.getId());
    }

    @Override
    public boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
        if (taskStates.isEmpty()) {
            return false;
        }
        for (TaskId id : tasks) {
            TaskState state = taskStates.get(id);
            if (state == null || !state.hasPassed(until)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean areTasksCompleted(Set<TaskId> tasks) {
        if (taskStates.isEmpty()) {
            return false;
        }
        for (TaskId id : tasks) {
            TaskState state = taskStates.get(id);
            if (state == null || !state.hasReachedEnd()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void configureWorkers(GroupedTasks workerTasks) throws InterruptedException {
        Map<TaskId, SortedSet<StreamId>> tasks = workerTasks.getTasks();

        // Remove task states for tasks no longer in the configuration
        Iterator<TaskId> it = taskStates.keySet().iterator();
        while (it.hasNext()) {
            if (!tasks.containsKey(it.next())) {
                it.remove();
            }
        }

        currentGenerationId = Optional.ofNullable(workerTasks.getGenerationId());

        // Stop current worker if exists
        stopWorkerThread();

        // Create and start a new worker
        startNewWorkerThread(workerTasks);
    }

    @Override
    public void configureWorkers(TableName tableName, GroupedTasks workerTasks) throws InterruptedException {
        Map<TaskId, SortedSet<StreamId>> tasks = workerTasks.getTasks();

        // Remove all existing tasks from taskStates that belong to this table and no longer in the configuration
        Iterator<TaskId> it = taskStates.keySet().iterator();
        while (it.hasNext()) {
            TaskId taskId = it.next();
            if (taskId.getTable().equals(tableName) && !tasks.containsKey(taskId)) {
                it.remove();
            }
        }

        // Update generation metadata for this table
        currentGenerationByTable.put(tableName, new TableGenerationState(workerTasks.getGenerationMetadata()));

        if (currentWorker == null) {
            // No worker exists, start a new one
            startNewWorkerThread(workerTasks);
        } else {
            if (!tasks.isEmpty()) {
                try {
                    currentWorker.addTasks(workerTasks);
                } catch (ExecutionException e) {
                    logger.atSevere().withCause(e).log("Error adding tasks for table %s", tableName);
                    throw new RuntimeException("Error adding tasks", e);
                }
            }
        }
    }

    @Override
    public void stopWorkers() throws InterruptedException {
        stopWorkerThread();
    }

    private void startNewWorkerThread(GroupedTasks workerTasks) {
        WorkerConfiguration workerConfiguration = workerConfigurationBuilder
                .withTransport(this)
                .withExecutorService(executorServiceSupplier.get())
                .build();

        currentWorker = new Worker(workerConfiguration);
        workerThread = new Thread(workersThreadGroup, () -> {
            try {
                currentWorker.run(workerTasks);
            } catch (InterruptedException | ExecutionException e) {
                logger.atSevere().withCause(e).log("Unhandled exception in worker thread");
            }
        });
        workerThread.start();
    }

    @Override
    public Map<TaskId, TaskState> getTaskStates(Set<TaskId> tasks) {
        Map<TaskId, TaskState> result = new HashMap<>();
        tasks.forEach(task -> {
            TaskState taskState = taskStates.get(task);
            if (taskState != null) {
                result.put(task, taskState);
            }
        });
        return result;
    }

    @Override
    public void setState(TaskId task, TaskState newState) {
        taskStates.put(task, newState);

        Optional<Timestamp> lastConsumed = newState.getLastConsumedChangeDate().map(Timestamp::new);
        if (lastConsumed.isPresent()) {
            updateMaxConsumedTimestamp(task.getTable(), task.getGenerationId(), lastConsumed.get());
        }
    }

    @Override
    public void moveStateToNextWindow(TaskId task, TaskState newState) {
        setState(task, newState);
    }

    private void stopWorkerThread() throws InterruptedException {
        if (currentWorker != null) {
            Worker workerToStop = currentWorker;
            Thread threadToJoin = workerThread;

            currentWorker = null;
            workerThread = null;

            workerToStop.stop();
            threadToJoin.join();
        }
    }

    public void stop() throws InterruptedException {
        stopWorkerThread();
    }

    public boolean isReadyToStart() {
        return currentWorker == null;
    }

    @Override
    public void updateGenerationMetadata(TableName table, GenerationMetadata metadata) {
        currentGenerationByTable.compute(table, (tbl, state) -> {
            if (state == null) {
                return new TableGenerationState(metadata);
            }
            if (!metadata.getId().equals(state.generationMetadata.getId())) {
                throw new IllegalArgumentException("Cannot update generation metadata for table " + table + " with a different ID: " + metadata.getId());
            }
            logger.atFine().log("Updating generation metadata for table %s: %s", table, metadata);
            state.generationMetadata = metadata;
            return state;
        });
    }

    @Override
    public Optional<Timestamp> getTableEndTimestamp(TableName table) {
        // if the table is not configured, return empty
        TableGenerationState state = currentGenerationByTable.get(table);
        if (state == null) {
            return Optional.empty();
        }
        return state.generationMetadata.getEnd();
    }

    public void updateMaxConsumedTimestamp(TableName table, GenerationId generationId, Timestamp timestamp) {
        currentGenerationByTable.compute(table, (tbl, state) -> {
            if (state != null && state.generationMetadata.getId().equals(generationId)) {
                if (!state.maxConsumedTimestamp.isPresent() || timestamp.compareTo(state.maxConsumedTimestamp.get()) > 0) {
                    state.maxConsumedTimestamp = Optional.of(timestamp);
                }
            }
            // else: ignore if generationId does not match
            return state;
        });
    }

    @Override
    public Optional<Timestamp> getMaxConsumedTimestamp(TableName table, GenerationId generationId) {
        TableGenerationState state = currentGenerationByTable.get(table);
        if (state == null) {
            return Optional.empty();
        }
        if (!state.generationMetadata.getId().equals(generationId)) {
            throw new IllegalArgumentException("GenerationId mismatch for table " + table + ": expected " + state.generationMetadata.getId() + ", got " + generationId);
        }
        return state.maxConsumedTimestamp;
    }
}
