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
 */
@PublicApi
public interface BaseNullableColumnValueSelector
{
  /**
   * Returns true if the primitive long, double, or float value returned by this selector should be treated as null.
   *
   * Users of {@link BaseLongColumnValueSelector#getLong()}, {@link BaseDoubleColumnValueSelector#getDouble()}
   * and {@link BaseFloatColumnValueSelector#getFloat()} must check this method first, or else they may improperly
   * use placeholder values returned by the primitive get methods.
   *
   * Users of {@link BaseObjectColumnValueSelector#getObject()} should not call this method. Instead, call "getObject"
   * and check if it is null.
   */
  @CalledFromHotLoop
  boolean isNull();
}
