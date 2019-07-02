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

package com.jackniu.flink.api.common.typeinfo;


 import com.jackniu.flink.annotations.Public;
 import com.jackniu.flink.annotations.PublicEvolving;
 import com.jackniu.flink.api.common.ExecutionConfig;
 import com.jackniu.flink.api.common.typeutils.TypeSerializer;
 import com.jackniu.flink.types.Nothing;

 /**
  * Placeholder type information for the {@link Nothing} type.
  */
 @Public
 public class NothingTypeInfo extends TypeInformation<Nothing> {

     private static final long serialVersionUID = 1L;

     @Override
     @PublicEvolving
     public boolean isBasicType() {
         return false;
     }

     @Override
     @PublicEvolving
     public boolean isTupleType() {
         return false;
     }

     @Override
     @PublicEvolving
     public int getArity() {
         return 0;
     }

     @Override
     @PublicEvolving
     public int getTotalFields() {
         return 1;
     }

     @Override
     @PublicEvolving
     public Class<Nothing> getTypeClass() {
         return Nothing.class;
     }

     @Override
     @PublicEvolving
     public boolean isKeyType() {
         return false;
     }

     @Override
     @PublicEvolving
     public TypeSerializer<Nothing> createSerializer(ExecutionConfig executionConfig) {
         throw new RuntimeException("The Nothing type cannot have a serializer.");
     }

     @Override
     public String toString() {
         return getClass().getSimpleName();
     }

     @Override
     public boolean equals(Object obj) {
         if (obj instanceof NothingTypeInfo) {
             NothingTypeInfo nothingTypeInfo = (NothingTypeInfo) obj;

             return nothingTypeInfo.canEqual(this);
         } else {
             return false;
         }
     }

     @Override
     public int hashCode() {
         return NothingTypeInfo.class.hashCode();
     }

     @Override
     public boolean canEqual(Object obj) {
         return obj instanceof NothingTypeInfo;
     }
 }