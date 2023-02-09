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
 * Provides indexes and information about them ({@link ColumnIndexCapabilities}) for a column. Indexes which satisfy
 * the {@link org.apache.druid.query.filter.Filter} used in a {@link org.apache.druid.query.Query}, allow the query
 * engine to construct {@link org.apache.druid.segment.data.Offset} (and
 * {@link org.apache.druid.segment.vector.VectorOffset}) to build {@link org.apache.druid.segment.Cursor}
 * (and {@link org.apache.druid.segment.vector.VectorCursor}). This allows the engine to only scan the rows which match
 * the filter values, instead of performing a full scan of the column and using a
 * {@link org.apache.druid.query.filter.ValueMatcher}
 * (or {@link org.apache.druid.query.filter.vector.VectorValueMatcher}) to filter the values.
 */
public interface ColumnIndexSupplier
{
  /**
   * Try to get a column 'index' of the specified type. If the index of the desired type is not available, this method
   * will return null. If the value is non-null, the index may be used for the eventual construction of an
   * {@link org.apache.druid.segment.data.Offset} to form the basis of a {@link org.apache.druid.segment.Cursor}
   * (or {@link org.apache.druid.segment.vector.VectorOffset} and {@link org.apache.druid.segment.vector.VectorCursor})
   * which can greatly reduce the total number of rows which need to be scanned and processed.
   *
   * Objects returned by this method are not thread-safe.
   */
  @Nullable
  <T> T as(Class<T> clazz);
}
