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

package org.apache.druid.query.filter;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.column.ColumnIndexSupplier;

import javax.annotation.Nullable;

/**
 */
public interface ColumnIndexSelector extends ColumnSelector
{
  int getNumRows();

  BitmapFactory getBitmapFactory();

  /**
   * Get the {@link ColumnIndexSupplier} of a column. If the column exists, but does not support indexes, this method
   * will return a non-null index supplier, likely {@link org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier}.
   * Columns which are 'missing' will return a null value from this method, which allows for filters to act on this
   * information to produce an all true or all false index depending on how the filter matches the null value.
   */
  @Nullable
  ColumnIndexSupplier getIndexSupplier(String column);
}
