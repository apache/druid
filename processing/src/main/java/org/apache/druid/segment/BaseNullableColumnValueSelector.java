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

package org.apache.druid.segment;

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;

/**
 * Null value checking polymorphic "part" of the {@link ColumnValueSelector} interface for primitive values.
 * Users of {@link BaseLongColumnValueSelector#getLong()}, {@link BaseDoubleColumnValueSelector#getDouble()}
 * and {@link BaseFloatColumnValueSelector#getFloat()} are required to check the nullability of the primitive
 * types returned.
 */
@PublicApi
public interface BaseNullableColumnValueSelector
{
  /**
   * Returns true if selected primitive value is null for {@link BaseFloatColumnValueSelector},
   * {@link BaseLongColumnValueSelector} and {@link BaseDoubleColumnValueSelector} otherwise false.
   */
  @CalledFromHotLoop
  boolean isNull();
}
