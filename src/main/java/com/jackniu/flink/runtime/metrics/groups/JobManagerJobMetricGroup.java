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

package com.jackniu.flink.runtime.metrics.groups;



import com.jackniu.flink.annotations.Internal;
import com.jackniu.flink.api.common.JobID;
import com.jackniu.flink.runtime.metrics.MetricRegistry;

import javax.annotation.Nullable;
import java.util.Collections;

import static com.jackniu.flink.util.Preconditions.checkNotNull;


/**
 * Special {@link } representing everything belonging to
 * a specific job, running on the JobManager.
 */
@Internal
public class JobManagerJobMetricGroup extends JobMetricGroup<JobManagerMetricGroup> {
	public JobManagerJobMetricGroup(
			MetricRegistry registry,
			JobManagerMetricGroup parent,
			JobID jobId,
			@Nullable String jobName) {
		super(registry, checkNotNull(parent), jobId, jobName, registry.getScopeFormats().getJobManagerJobFormat().formatScope(checkNotNull(parent), jobId, jobName));
	}

	public final JobManagerMetricGroup parent() {
		return parent;
	}

	// ------------------------------------------------------------------------
	//  Component Metric Group Specifics
	// ------------------------------------------------------------------------

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return Collections.emptyList();
	}
}
