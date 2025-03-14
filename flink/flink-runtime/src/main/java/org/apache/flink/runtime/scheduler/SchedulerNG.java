/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for scheduling Flink jobs.
 *
 * <p>Instances are created via {@link SchedulerNGFactory}, and receive a {@link JobGraph} when
 * instantiated.
 *
 * <p>Implementations can expect that methods will not be invoked concurrently. In fact, all
 * invocations will originate from a thread in the {@link ComponentMainThreadExecutor}.
 */
public interface SchedulerNG extends GlobalFailureHandler, AutoCloseableAsync {

    void startScheduling();

    void cancel();

    CompletableFuture<JobStatus> getJobTerminationFuture();

    default boolean updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        return updateTaskExecutionState(new TaskExecutionStateTransition(taskExecutionState));
    }

    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState);

    SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException;

    ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException;

    ExecutionGraphInfo requestJob();

    /**
     * Returns the checkpoint statistics for a given job. Although the {@link
     * CheckpointStatsSnapshot} is included in the {@link ExecutionGraphInfo}, this method is
     * preferred to {@link SchedulerNG#requestJob()} because it is less expensive.
     *
     * @return checkpoint statistics snapshot for job graph
     */
    CheckpointStatsSnapshot requestCheckpointStats();

    JobStatus requestJobStatus();

    JobDetails requestJobDetails();

    // ------------------------------------------------------------------------------------
    // Methods below do not belong to Scheduler but are included due to historical reasons
    // ------------------------------------------------------------------------------------

    KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException;

    void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException;

    void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException;

    // ------------------------------------------------------------------------

    void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot);

    // ------------------------------------------------------------------------

    CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob, SavepointFormatType formatType);

    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType);

    void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState);

    void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics);

    void declineCheckpoint(DeclineCheckpoint decline);

    CompletableFuture<String> stopWithSavepoint(
            String targetDirectory, boolean terminate, SavepointFormatType formatType);

    // ------------------------------------------------------------------------
    //  Operator Coordinator related methods
    //
    //  These are necessary as long as the Operator Coordinators are part of the
    //  scheduler. There are good reasons to pull them out of the Scheduler and
    //  make them directly a part of the JobMaster. However, we would need to
    //  rework the complete CheckpointCoordinator initialization before we can
    //  do that, because the CheckpointCoordinator is initialized (and restores
    //  savepoint) in the scheduler constructor, which requires the coordinators
    //  to be there as well.
    // ------------------------------------------------------------------------

    /**
     * Delivers the given OperatorEvent to the {@link OperatorCoordinator} with the given {@link
     * OperatorID}.
     *
     * <p>Failure semantics: If the task manager sends an event for a non-running task or a
     * non-existing operator coordinator, then respond with an exception to the call. If task and
     * coordinator exist, then we assume that the call from the TaskManager was valid, and any
     * bubbling exception needs to cause a job failure
     *
     * @throws FlinkException Thrown, if the task is not running or no operator/coordinator exists
     *     for the given ID.
     */
    void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecution, OperatorID operator, OperatorEvent evt)
            throws FlinkException;

    /**
     * Delivers a coordination request to the {@link OperatorCoordinator} with the given {@link
     * OperatorID} and returns the coordinator's response.
     *
     * @return A future containing the response.
     * @throws FlinkException Thrown, if the task is not running, or no operator/coordinator exists
     *     for the given ID, or the coordinator cannot handle client events.
     */
    CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException;

    /**
     * Notifies that the task has reached the end of data.
     *
     * @param executionAttemptID The execution attempt id.
     */
    void notifyEndOfData(ExecutionAttemptID executionAttemptID);

    /**
     * Read current {@link JobResourceRequirements job resource requirements}.
     *
     * @return Current resource requirements.
     */
    default JobResourceRequirements requestJobResourceRequirements() {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s does not support changing the parallelism without a job restart. This feature is currently only expected to work with the %s.",
                        getClass().getSimpleName(), AdaptiveScheduler.class.getSimpleName()));
    }

    /**
     * Update {@link JobResourceRequirements job resource requirements}.
     *
     * @param jobResourceRequirements new resource requirements
     */
    default void updateJobResourceRequirements(JobResourceRequirements jobResourceRequirements) {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s does not support changing the parallelism without a job restart. This feature is currently only expected to work with the %s.",
                        getClass().getSimpleName(), AdaptiveScheduler.class.getSimpleName()));
    }

    /**
     * Read current {@link JustinResourceRequirements job resource requirements}.
     *
     * @return Current resource requirements.
     */
    default JustinResourceRequirements requestJustinResourceRequirements() {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s does not support changing the parallelism without a job restart. This feature is currently only expected to work with the %s.",
                        getClass().getSimpleName(), AdaptiveScheduler.class.getSimpleName()));
    }
    /**
     * Update {@link JustinResourceRequirements job resource requirements}.
     *
     * @param justinResourceRequirements new resource requirements
     */
    default void updateJustinResourceRequirements(
            JustinResourceRequirements justinResourceRequirements) {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s does not support changing the parallelism without a job restart. This feature is currently only expected to work with the %s.",
                        getClass().getSimpleName(), AdaptiveScheduler.class.getSimpleName()));
    }
}
