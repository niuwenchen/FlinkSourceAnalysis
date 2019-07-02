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

package com.jackniu.flink.api.common.typeutils.base;


import com.jackniu.flink.annotations.Internal;
import com.jackniu.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import com.jackniu.flink.api.common.typeutils.TypeComparator;
import com.jackniu.flink.api.common.typeutils.TypeSerializerSnapshot;
import com.jackniu.flink.core.memory.DataInputView;
import com.jackniu.flink.core.memory.DataOutputView;
import com.jackniu.flink.core.memory.MemorySegment;
import com.jackniu.flink.types.ByteValue;
import com.jackniu.flink.types.NormalizableKey;

import java.io.IOException;

@Internal
public final class ByteValueSerializer extends TypeSerializerSingleton<ByteValue> {

	private static final long serialVersionUID = 1L;
	
	public static final ByteValueSerializer INSTANCE = new ByteValueSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public ByteValue createInstance() {
		return new ByteValue();
	}

	@Override
	public ByteValue copy(ByteValue from) {
		return copy(from, new ByteValue());
	}
	
	@Override
	public ByteValue copy(ByteValue from, ByteValue reuse) {
		reuse.setValue(from.getValue());
		return reuse;
	}

	@Override
	public int getLength() {
		return 1;
	}

	@Override
	public void serialize(ByteValue record, DataOutputView target) throws IOException {
		record.write(target);
	}

	@Override
	public ByteValue deserialize(DataInputView source) throws IOException {
		return deserialize(new ByteValue(), source);
	}
	
	@Override
	public ByteValue deserialize(ByteValue reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeByte(source.readByte());
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ByteValueSerializer;
	}

	@Override
	public TypeSerializerSnapshot<ByteValue> snapshotConfiguration() {
		return new ByteValueSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	public static final class ByteValueSerializerSnapshot extends SimpleTypeSerializerSnapshot<ByteValue> {

		public ByteValueSerializerSnapshot() {
			super(ByteValueSerializer.class);
		}
	}
}
