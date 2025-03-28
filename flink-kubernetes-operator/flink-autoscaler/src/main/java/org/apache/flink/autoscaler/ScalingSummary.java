/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/** Scaling summary returned by the {@link ScalingMetricEvaluator}. */
@Data
@NoArgsConstructor
public class ScalingSummary {

    private int currentParallelism;

    private int newParallelism;

    private ResourceProfile currentResourceProfile;

    private ResourceProfile newResourceProfile;

    private Map<ScalingMetric, EvaluatedScalingMetric> metrics;

    public ScalingSummary(
            int currentParallelism,
            int newParallelism,
            Map<ScalingMetric, EvaluatedScalingMetric> metrics) {
        if (currentParallelism == newParallelism) {
            //throw new IllegalArgumentException(
            //        "Current parallelism should not be equal to newParallelism during scaling.");
        }
        this.currentParallelism = currentParallelism;
        this.newParallelism = newParallelism;
        this.metrics = metrics;
    }

    public void setNewResourceProfile(ResourceProfile rp) {
        this.newResourceProfile = rp;
    }


    @JsonIgnore
    public boolean isScaledUp() {
        return newParallelism > currentParallelism;
    }
}
