/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jackniu.flink.runtime.operators.hash;

import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypePairComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.core.memory.MemorySegment;
import com.jackniu.flink.runtime.io.disk.iomanager.IOManager;
import com.jackniu.flink.runtime.jobgraph.tasks.AbstractInvokable;
import com.jackniu.flink.runtime.memory.MemoryAllocationException;
import com.jackniu.flink.runtime.memory.MemoryManager;

import java.util.List;

/**
 * Common methods for all Hash Join Iterators.
 */
public class HashJoinIteratorBase {
	
	public <BT, PT> MutableHashTable<BT, PT> getHashJoin(
			TypeSerializer<BT> buildSideSerializer,
			TypeComparator<BT> buildSideComparator,
			TypeSerializer<PT> probeSideSerializer,
			TypeComparator<PT> probeSideComparator,
			TypePairComparator<PT, BT> pairComparator,
			MemoryManager memManager,
			IOManager ioManager,
			AbstractInvokable ownerTask,
			double memoryFraction,
			boolean useBloomFilters) throws MemoryAllocationException {

		final int numPages = memManager.computeNumberOfPages(memoryFraction);
		final List<MemorySegment> memorySegments = memManager.allocatePages(ownerTask, numPages);
		
		return new MutableHashTable<BT, PT>(buildSideSerializer, probeSideSerializer,
				buildSideComparator, probeSideComparator, pairComparator,
				memorySegments, ioManager,
				useBloomFilters);
	}
}