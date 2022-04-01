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

public interface ColumnIndexSupplier
{
  /**
   * Get the {@link ColumnIndexCapabilities} for the specified type of index. If the index does not exist
   * this method will return null. A null return value from this method indicates that an index of the desired type
   * in unavailable
   */
  @Nullable
  <T> ColumnIndexCapabilities getIndexCapabilities(Class<T> clazz);

  /**
   * Get a column 'index' of the specified type. If the index of the desired type is not available, this method will
   * return null
   */
  @Nullable
  <T> T getIndex(Class<T> clazz);
}
