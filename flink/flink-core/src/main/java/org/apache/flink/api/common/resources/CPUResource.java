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

package org.apache.flink.api.common.resources;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;

import static java.math.BigDecimal.ROUND_HALF_UP;

/** Represents CPU resource. */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class CPUResource extends Resource<CPUResource> {

    private static final long serialVersionUID = 7228645888210984393L;

    public static final String NAME = "CPU";

    public CPUResource(double value) {
        super(NAME, value);
    }

    private CPUResource(BigDecimal value) {
        super(NAME, value);
    }

    public CPUResource(int value) {
        super(NAME, (double) value);
    }

    public CPUResource() {
        super(NAME, 1.0);
    }

    @Override
    public CPUResource create(BigDecimal value) {
        return new CPUResource(value);
    }

    public String toHumanReadableString() {
        return String.format(
                "%.2f cores", getValue().setScale(2, ROUND_HALF_UP).stripTrailingZeros());
    }
}
