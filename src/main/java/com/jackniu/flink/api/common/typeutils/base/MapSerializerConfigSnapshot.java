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
import com.jackniu.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import com.jackniu.flink.api.common.typeutils.TypeSerializer;
import com.jackniu.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;

import java.util.Map;

/**
 * Configuration snapshot for serializers of maps, containing the
 * configuration snapshot of its key serializer and value serializer.
 *
 * @deprecated this snapshot class should not be used by any serializer anymore.
 */
@Internal
@Deprecated
public final class MapSerializerConfigSnapshot<K, V> extends CompositeTypeSerializerConfigSnapshot<Map<K, V>> {

	private static final int VERSION = 1;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public MapSerializerConfigSnapshot() {}

	public MapSerializerConfigSnapshot(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
		super(keySerializer, valueSerializer);
	}

	@Override
	public TypeSerializerSchemaCompatibility<Map<K, V>> resolveSchemaCompatibility(TypeSerializer<Map<K, V>> newSerializer) {
		if (newSerializer instanceof MapSerializer) {
			// redirect the compatibility check to the new MapSerializerConfigSnapshot
			MapSerializer<K, V> mapSerializer = (MapSerializer<K, V>) newSerializer;

			MapSerializerSnapshot<K, V> mapSerializerSnapshot =
				new MapSerializerSnapshot<>(mapSerializer.getKeySerializer(), mapSerializer.getValueSerializer());
			return mapSerializerSnapshot.resolveSchemaCompatibility(newSerializer);
		}
		else {
			return super.resolveSchemaCompatibility(newSerializer);
		}
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
