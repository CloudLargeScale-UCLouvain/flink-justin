/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.justin.JustinResourceRequirements;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/** In-Memory implementation of {@link JobGraphStore} for testing purposes. */
public class TestingJobGraphStore implements JobGraphStore {

    private final Map<JobID, JobGraph> storedJobs = new HashMap<>();

    private final ThrowingConsumer<JobGraphListener, ? extends Exception> startConsumer;

    private final ThrowingRunnable<? extends Exception> stopRunnable;

    private final FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
            jobIdsFunction;

    private final BiFunctionWithException<
                    JobID, Map<JobID, JobGraph>, JobGraph, ? extends Exception>
            recoverJobGraphFunction;

    private final ThrowingConsumer<JobGraph, ? extends Exception> putJobGraphConsumer;

    private final BiConsumerWithException<JobGraph, JobResourceRequirements, ? extends Exception>
            putJobResourceRequirementsConsumer;

    private final BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction;

    private final BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction;

    private boolean started;

    private TestingJobGraphStore(
            ThrowingConsumer<JobGraphListener, ? extends Exception> startConsumer,
            ThrowingRunnable<? extends Exception> stopRunnable,
            FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                    jobIdsFunction,
            BiFunctionWithException<JobID, Map<JobID, JobGraph>, JobGraph, ? extends Exception>
                    recoverJobGraphFunction,
            ThrowingConsumer<JobGraph, ? extends Exception> putJobGraphConsumer,
            BiConsumerWithException<JobGraph, JobResourceRequirements, ? extends Exception>
                    putJobResourceRequirementsConsumer,
            BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction,
            BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction,
            Collection<JobGraph> initialJobGraphs) {
        this.startConsumer = startConsumer;
        this.stopRunnable = stopRunnable;
        this.jobIdsFunction = jobIdsFunction;
        this.recoverJobGraphFunction = recoverJobGraphFunction;
        this.putJobGraphConsumer = putJobGraphConsumer;
        this.putJobResourceRequirementsConsumer = putJobResourceRequirementsConsumer;
        this.globalCleanupFunction = globalCleanupFunction;
        this.localCleanupFunction = localCleanupFunction;

        for (JobGraph initialJobGraph : initialJobGraphs) {
            storedJobs.put(initialJobGraph.getJobID(), initialJobGraph);
        }
    }

    @Override
    public synchronized void start(@Nullable JobGraphListener jobGraphListener) throws Exception {
        startConsumer.accept(jobGraphListener);
        started = true;
    }

    @Override
    public synchronized void stop() throws Exception {
        stopRunnable.run();
        started = false;
    }

    @Override
    public synchronized JobGraph recoverJobGraph(JobID jobId) throws Exception {
        verifyIsStarted();
        return recoverJobGraphFunction.apply(jobId, storedJobs);
    }

    @Override
    public synchronized void putJobGraph(JobGraph jobGraph) throws Exception {
        verifyIsStarted();
        putJobGraphConsumer.accept(jobGraph);
        storedJobs.put(jobGraph.getJobID(), jobGraph);
    }

    @Override
    public void putJobResourceRequirements(
            JobID jobId, JobResourceRequirements jobResourceRequirements) throws Exception {
        verifyIsStarted();
        final JobGraph jobGraph =
                Preconditions.checkNotNull(storedJobs.get(jobId), "Job [%s] not found.", jobId);
        putJobResourceRequirementsConsumer.accept(jobGraph, jobResourceRequirements);
    }

    @Override
    public void putJustinResourceRequirements(
            JobID jobId,
            JustinResourceRequirements justinResourceRequirements) throws Exception {

    }

    @Override
    public synchronized CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        verifyIsStarted();
        return globalCleanupFunction.apply(jobId, executor).thenRun(() -> storedJobs.remove(jobId));
    }

    @Override
    public synchronized CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        verifyIsStarted();
        return localCleanupFunction.apply(jobId, executor);
    }

    @Override
    public synchronized Collection<JobID> getJobIds() throws Exception {
        verifyIsStarted();
        return jobIdsFunction.apply(
                Collections.unmodifiableSet(new HashSet<>(storedJobs.keySet())));
    }

    public synchronized boolean contains(JobID jobId) {
        return storedJobs.containsKey(jobId);
    }

    private void verifyIsStarted() {
        Preconditions.checkState(started, "Not running. Forgot to call start()?");
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingJobGraphStore} instances. */
    public static class Builder {
        private ThrowingConsumer<JobGraphListener, ? extends Exception> startConsumer =
                ignored -> {};

        private ThrowingRunnable<? extends Exception> stopRunnable = () -> {};

        private FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                jobIdsFunction = jobIds -> jobIds;

        private BiFunctionWithException<JobID, Map<JobID, JobGraph>, JobGraph, ? extends Exception>
                recoverJobGraphFunction = (jobId, jobs) -> jobs.get(jobId);

        private ThrowingConsumer<JobGraph, ? extends Exception> putJobGraphConsumer = ignored -> {};

        private BiConsumerWithException<JobGraph, JobResourceRequirements, ? extends Exception>
                putJobResourceRequirementsConsumer = (graph, requirements) -> {};

        private BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction =
                (ignoredJobId, ignoredExecutor) -> FutureUtils.completedVoidFuture();

        private BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction =
                (ignoredJobId, ignoredExecutor) -> FutureUtils.completedVoidFuture();

        private Collection<JobGraph> initialJobGraphs = Collections.emptyList();

        private boolean startJobGraphStore = false;

        private Builder() {}

        public Builder setStartConsumer(
                ThrowingConsumer<JobGraphListener, ? extends Exception> startConsumer) {
            this.startConsumer = startConsumer;
            return this;
        }

        public Builder setStopRunnable(ThrowingRunnable<? extends Exception> stopRunnable) {
            this.stopRunnable = stopRunnable;
            return this;
        }

        public Builder setJobIdsFunction(
                FunctionWithException<Collection<JobID>, Collection<JobID>, ? extends Exception>
                        jobIdsFunction) {
            this.jobIdsFunction = jobIdsFunction;
            return this;
        }

        public Builder setRecoverJobGraphFunction(
                BiFunctionWithException<JobID, Map<JobID, JobGraph>, JobGraph, ? extends Exception>
                        recoverJobGraphFunction) {
            this.recoverJobGraphFunction = recoverJobGraphFunction;
            return this;
        }

        public Builder setPutJobGraphConsumer(
                ThrowingConsumer<JobGraph, ? extends Exception> putJobGraphConsumer) {
            this.putJobGraphConsumer = putJobGraphConsumer;
            return this;
        }

        public Builder setPutJobResourceRequirementsConsumer(
                BiConsumerWithException<JobGraph, JobResourceRequirements, ? extends Exception>
                        putJobResourceRequirementsConsumer) {
            this.putJobResourceRequirementsConsumer = putJobResourceRequirementsConsumer;
            return this;
        }

        public Builder setGlobalCleanupFunction(
                BiFunction<JobID, Executor, CompletableFuture<Void>> globalCleanupFunction) {
            this.globalCleanupFunction = globalCleanupFunction;
            return this;
        }

        public Builder setLocalCleanupFunction(
                BiFunction<JobID, Executor, CompletableFuture<Void>> localCleanupFunction) {
            this.localCleanupFunction = localCleanupFunction;
            return this;
        }

        public Builder setInitialJobGraphs(Collection<JobGraph> initialJobGraphs) {
            this.initialJobGraphs = initialJobGraphs;
            return this;
        }

        public Builder withAutomaticStart() {
            this.startJobGraphStore = true;
            return this;
        }

        public TestingJobGraphStore build() {
            final TestingJobGraphStore jobGraphStore =
                    new TestingJobGraphStore(
                            startConsumer,
                            stopRunnable,
                            jobIdsFunction,
                            recoverJobGraphFunction,
                            putJobGraphConsumer,
                            putJobResourceRequirementsConsumer,
                            globalCleanupFunction,
                            localCleanupFunction,
                            initialJobGraphs);

            if (startJobGraphStore) {
                try {
                    jobGraphStore.start(null);
                } catch (Exception e) {
                    ExceptionUtils.rethrow(e);
                }
            }

            return jobGraphStore;
        }
    }
}
