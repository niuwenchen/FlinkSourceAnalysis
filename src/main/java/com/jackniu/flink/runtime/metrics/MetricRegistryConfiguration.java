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


import com.jackniu.flink.api.java.tuple.Tuple2;
import com.jackniu.flink.configuration.ConfigConstants;
import com.jackniu.flink.configuration.Configuration;
import com.jackniu.flink.configuration.DelegatingConfiguration;
import com.jackniu.flink.configuration.MetricOptions;
import com.jackniu.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Configuration object for {@link MetricRegistryImpl}.
 */
public class MetricRegistryConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(MetricRegistryConfiguration.class);

	private static volatile MetricRegistryConfiguration defaultConfiguration;

	// regex pattern to split the defined reporters
	private static final Pattern reporterListPattern = Pattern.compile("\\s*,\\s*");

	// regex pattern to extract the name from reporter configuration keys, e.g. "rep" from "metrics.reporter.rep.class"
	private static final Pattern reporterClassPattern = Pattern.compile(
		Pattern.quote(ConfigConstants.METRICS_REPORTER_PREFIX) +
		// [\S&&[^.]] = intersection of non-whitespace and non-period character classes
		"([\\S&&[^.]]*)\\." +
		Pattern.quote(ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX));

	// scope formats for the different components
	private final ScopeFormats scopeFormats;

	// delimiter for the scope strings
	private final char delimiter;

	// contains for every configured reporter its name and the configuration object
	private final List<Tuple2<String, Configuration>> reporterConfigurations;

	private final long queryServiceMessageSizeLimit;

	public MetricRegistryConfiguration(
		ScopeFormats scopeFormats,
		char delimiter,
		List<Tuple2<String, Configuration>> reporterConfigurations,
		long queryServiceMessageSizeLimit) {

		this.scopeFormats = Preconditions.checkNotNull(scopeFormats);
		this.delimiter = delimiter;
		this.reporterConfigurations = Preconditions.checkNotNull(reporterConfigurations);
		this.queryServiceMessageSizeLimit = queryServiceMessageSizeLimit;
	}

	// ------------------------------------------------------------------------
	//  Getter
	// ------------------------------------------------------------------------

	public ScopeFormats getScopeFormats() {
		return scopeFormats;
	}

	public char getDelimiter() {
		return delimiter;
	}

	public List<Tuple2<String, Configuration>> getReporterConfigurations() {
		return reporterConfigurations;
	}

	public long getQueryServiceMessageSizeLimit() {
		return queryServiceMessageSizeLimit;
	}

	// ------------------------------------------------------------------------
	//  Static factory methods
	// ------------------------------------------------------------------------

	/**
	 * Create a metric registry configuration object from the given {@link Configuration}.
	 *
	 * @param configuration to generate the metric registry configuration from
	 * @return Metric registry configuration generated from the configuration
	 */
	public static MetricRegistryConfiguration fromConfiguration(Configuration configuration) {
		ScopeFormats scopeFormats;
		try {
			scopeFormats = ScopeFormats.fromConfig(configuration);
		} catch (Exception e) {
			LOG.warn("Failed to parse scope format, using default scope formats", e);
			scopeFormats = ScopeFormats.fromConfig(new Configuration());
		}

		char delim;
		try {
			delim = configuration.getString(MetricOptions.SCOPE_DELIMITER).charAt(0);
		} catch (Exception e) {
			LOG.warn("Failed to parse delimiter, using default delimiter.", e);
			delim = '.';
		}

		String includedReportersString = configuration.getString(MetricOptions.REPORTERS_LIST, "");
		Set<String> includedReporters = reporterListPattern.splitAsStream(includedReportersString)
			.collect(Collectors.toSet());

		// use a TreeSet to make the reporter order deterministic, which is useful for testing
		Set<String> namedReporters = new TreeSet<>(String::compareTo);
		// scan entire configuration for "metric.reporter" keys and parse individual reporter configurations
		for (String key : configuration.keySet()) {
			if (key.startsWith(ConfigConstants.METRICS_REPORTER_PREFIX)) {
				Matcher matcher = reporterClassPattern.matcher(key);
				if (matcher.matches()) {
					String reporterName = matcher.group(1);
					if (includedReporters.isEmpty() || includedReporters.contains(reporterName)) {
						if (namedReporters.contains(reporterName)) {
							LOG.warn("Duplicate class configuration detected for reporter {}.", reporterName);
						} else {
							namedReporters.add(reporterName);
						}
					} else {
						LOG.info("Excluding reporter {}, not configured in reporter list ({}).", reporterName, includedReportersString);
					}
				}
			}
		}

		List<Tuple2<String, Configuration>> reporterConfigurations;

		if (namedReporters.isEmpty()) {
			reporterConfigurations = Collections.emptyList();
		} else {
			reporterConfigurations = new ArrayList<>(namedReporters.size());

			for (String namedReporter: namedReporters) {
				DelegatingConfiguration delegatingConfiguration = new DelegatingConfiguration(
					configuration,
					ConfigConstants.METRICS_REPORTER_PREFIX + namedReporter + '.');

				reporterConfigurations.add(Tuple2.of(namedReporter, (Configuration) delegatingConfiguration));
			}
		}

		final String maxFrameSizeStr = configuration.getString(AkkaOptions.FRAMESIZE);
		final String akkaConfigStr = String.format("akka {remote {netty.tcp {maximum-frame-size = %s}}}", maxFrameSizeStr);
		final Config akkaConfig = ConfigFactory.parseString(akkaConfigStr);
		final long maximumFrameSize = akkaConfig.getBytes("akka.remote.netty.tcp.maximum-frame-size");

		// padding to account for serialization overhead
		final long messageSizeLimitPadding = 256;

		return new MetricRegistryConfiguration(scopeFormats, delim, reporterConfigurations, maximumFrameSize - messageSizeLimitPadding);
	}

	public static MetricRegistryConfiguration defaultMetricRegistryConfiguration() {
		// create the default metric registry configuration only once
		if (defaultConfiguration == null) {
			synchronized (MetricRegistryConfiguration.class) {
				if (defaultConfiguration == null) {
					defaultConfiguration = fromConfiguration(new Configuration());
				}
			}
		}

		return defaultConfiguration;
	}

}
