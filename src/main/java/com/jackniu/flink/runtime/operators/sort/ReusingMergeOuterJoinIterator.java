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

package com.jackniu.flink.runtime.operators.sort;

import com.jackniu.flink.api.common.operators.base.OuterJoinOperatorBase.OuterJoinType;
import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypePairComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.runtime.io.disk.iomanager.IOManager;
import com.jackniu.flink.runtime.jobgraph.tasks.AbstractInvokable;
import com.jackniu.flink.runtime.memory.MemoryAllocationException;
import com.jackniu.flink.runtime.memory.MemoryManager;
import com.jackniu.flink.runtime.util.KeyGroupedIterator;
import com.jackniu.flink.runtime.util.ReusingKeyGroupedIterator;
import com.jackniu.flink.util.MutableObjectIterator;

public class ReusingMergeOuterJoinIterator<T1, T2, O> extends AbstractMergeOuterJoinIterator<T1, T2, O> {

	public ReusingMergeOuterJoinIterator(
			OuterJoinType outerJoinType,
			MutableObjectIterator<T1> input1,
			MutableObjectIterator<T2> input2,
			TypeSerializer<T1> serializer1, TypeComparator<T1> comparator1,
			TypeSerializer<T2> serializer2, TypeComparator<T2> comparator2,
			TypePairComparator<T1, T2> pairComparator,
			MemoryManager memoryManager,
			IOManager ioManager,
			int numMemoryPages,
			AbstractInvokable parentTask)
			throws MemoryAllocationException {
		super(outerJoinType, input1, input2, serializer1, comparator1, serializer2, comparator2, pairComparator, memoryManager, ioManager, numMemoryPages, parentTask);

		this.copy1 = serializer1.createInstance();
		this.spillHeadCopy = serializer1.createInstance();
		this.copy2 = serializer2.createInstance();
		this.blockHeadCopy = serializer2.createInstance();
	}

	@Override
	protected <T> KeyGroupedIterator<T> createKeyGroupedIterator(MutableObjectIterator<T> input, TypeSerializer<T> serializer, TypeComparator<T> comparator) {
		return new ReusingKeyGroupedIterator<>(input, serializer, comparator);
	}

	@Override
	protected <T> T createCopy(TypeSerializer<T> serializer, T value, T reuse) { return serializer.copy(value, reuse); }


}