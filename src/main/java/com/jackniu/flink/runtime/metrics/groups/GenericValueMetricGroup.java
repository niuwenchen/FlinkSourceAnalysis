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
import com.jackniu.flink.metrics.CharacterFilter;
import com.jackniu.flink.runtime.metrics.MetricRegistry;
import com.jackniu.flink.runtime.metrics.scope.ScopeFormat;

import java.util.Map;

/**
 * A {@link GenericMetricGroup} for representing the value part of a key-value metric group pair.
 *
 * @see GenericKeyMetricGroup
 * @see
 */
@Internal
public class GenericValueMetricGroup extends GenericMetricGroup {
	private String key;
	private final String value;

	GenericValueMetricGroup(MetricRegistry registry, GenericKeyMetricGroup parent, String value) {
		super(registry, parent, value);
		this.key = parent.getGroupName(name -> name);
		this.value = value;
	}

	// ------------------------------------------------------------------------

	@Override
	protected void putVariables(Map<String, String> variables) {
		variables.put(ScopeFormat.asVariable(this.key), value);
	}

	@Override
	public String getLogicalScope(CharacterFilter filter, char delimiter) {
		return parent.getLogicalScope(filter, delimiter);
	}
}
