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

package com.jackniu.flink.runtime.metrics;


import com.jackniu.flink.metrics.Metric;
import com.jackniu.flink.runtime.metrics.groups.AbstractMetricGroup;
import com.jackniu.flink.runtime.metrics.scope.ScopeFormats;

import javax.annotation.Nullable;

/**
 * Interface for a metric registry.
 */
public interface MetricRegistry {

	/**
	 * Returns the global delimiter.
	 *
	 * @return global delimiter
	 */
	char getDelimiter();

	/**
	 * Returns the configured delimiter for the reporter with the given index.
	 *
	 * @param index index of the reporter whose delimiter should be used
	 * @return configured reporter delimiter, or global delimiter if index is invalid
	 */
	char getDelimiter(int index);

	/**
	 * Returns the number of registered reporters.
	 */
	int getNumberReporters();

	/**
	 * Registers a new {@link Metric} with this registry.
	 *
	 * @param metric      the metric that was added
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	void register(Metric metric, String metricName, AbstractMetricGroup group);

	/**
	 * Un-registers the given {@link Metric} with this registry.
	 *
	 * @param metric      the metric that should be removed
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	void unregister(Metric metric, String metricName, AbstractMetricGroup group);

	/**
	 * Returns the scope formats.
	 *
	 * @return scope formats
	 */
	ScopeFormats getScopeFormats();

	/**
	 * Returns the path of the {@link } or null, if none is started.
	 *
	 * @return Path of the MetricQueryService or null, if none is started
	 */
	@Nullable
	String getMetricQueryServicePath();
}
