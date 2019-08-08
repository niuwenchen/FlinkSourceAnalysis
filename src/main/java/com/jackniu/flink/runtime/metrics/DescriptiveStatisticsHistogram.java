/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jackniu.flink.runtime.metrics;

import com.jackniu.flink.metrics.HistogramStatistics;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


/**
 * The {@link DescriptiveStatisticsHistogram} use a DescriptiveStatistics {@link DescriptiveStatistics} as a Flink {@link Histogram}.
 */
public class DescriptiveStatisticsHistogram implements com.jackniu.flink.metrics.Histogram {

	private final DescriptiveStatistics descriptiveStatistics;

	public DescriptiveStatisticsHistogram(int windowSize) {
		this.descriptiveStatistics = new DescriptiveStatistics(windowSize);
	}


	public void update(long value) {
		this.descriptiveStatistics.addValue(value);
	}


	public long getCount() {
		return this.descriptiveStatistics.getN();
	}


	public HistogramStatistics getStatistics() {
		return new DescriptiveStatisticsHistogramStatistics(this.descriptiveStatistics);
	}
}
