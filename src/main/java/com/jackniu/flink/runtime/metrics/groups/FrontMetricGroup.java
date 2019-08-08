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


import com.jackniu.flink.metrics.CharacterFilter;

public class FrontMetricGroup<P extends AbstractMetricGroup<?>> extends ProxyMetricGroup<P> {

	protected int reporterIndex;

	public FrontMetricGroup(int reporterIndex, P reference) {
		super(reference);
		this.reporterIndex = reporterIndex;
	}

	@Override
	public String getMetricIdentifier(String metricName) {
		return parentMetricGroup.getMetricIdentifier(metricName, null, this.reporterIndex);
	}

	@Override
	public String getMetricIdentifier(String metricName, CharacterFilter filter) {
		return parentMetricGroup.getMetricIdentifier(metricName, filter, this.reporterIndex);
	}

	public String getLogicalScope(CharacterFilter filter) {
		return parentMetricGroup.getLogicalScope(filter);
	}

	public String getLogicalScope(CharacterFilter filter, char delimiter) {
		return parentMetricGroup.getLogicalScope(filter, delimiter, this.reporterIndex);
	}
}
