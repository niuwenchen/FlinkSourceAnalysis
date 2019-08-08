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

package com.jackniu.flink.runtime.checkpoint;


import com.jackniu.flink.runtime.jobgraph.JobStatus;

/**
 * A checkpoint ID counter.
 */
public interface CheckpointIDCounter {

	/**
	 * Starts the {@link CheckpointIDCounter} service down.
	 */
	void start() throws Exception;

	/**
	 * Shuts the {@link CheckpointIDCounter} service.
	 *
	 * <p>The job status is forwarded and used to decide whether state should
	 * actually be discarded or kept.
	 *
	 * @param jobStatus Job state on shut down
	 */
	void shutdown(JobStatus jobStatus) throws Exception;

	/**
	 * Atomically increments the current checkpoint ID.
	 *
	 * @return The previous checkpoint ID
	 */
	long getAndIncrement() throws Exception;

	/**
	 * Sets the current checkpoint ID.
	 *
	 * @param newId The new ID
	 */
	void setCount(long newId) throws Exception;

}
