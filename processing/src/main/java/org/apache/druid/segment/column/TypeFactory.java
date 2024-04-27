/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.column;

import javax.annotation.Nullable;

/**
 * Create {@link TypeSignature} of a {@link TypeDescriptor}. Useful for creating types from
 * {@link TypeSignature#asTypeString()}} or converting between {@link TypeSignature} of different {@link TypeDescriptor}
 * implementations. Implementations also offer object interning for arbitrary array and complex types.
 */
public interface TypeFactory<Type extends TypeSignature<? extends TypeDescriptor>>
{
  Type ofString();
  Type ofFloat();
  Type ofDouble();
  Type ofLong();
  Type ofArray(Type elementType);
  Type ofComplex(@Nullable String complexTypeName);

  <T> TypeStrategy<T> getTypeStrategy(Type type);
}
