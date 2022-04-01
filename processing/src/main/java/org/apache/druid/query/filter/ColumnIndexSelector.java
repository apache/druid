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
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnIndexCapabilities;

import javax.annotation.Nullable;

/**
 */
public interface ColumnIndexSelector extends ColumnInspector
{
  int getNumRows();

  BitmapFactory getBitmapFactory();

  /**
   * Get the {@link ColumnIndexCapabilities} of a column for the specified type of index. If the index does not exist
   * this method will return null. Note that 'missing' columns should in fact return a non-null value from this method
   * to allow for filters to use 'nil' bitmaps if the filter matches nulls, in order to produce an all true or all
   * false index.
   */
  @Nullable
  <T> ColumnIndexCapabilities getIndexCapabilities(String column, Class<T> clazz);

  /**
   * Get the specified type of index for the specified column. {@link #getIndexCapabilities(String, Class)} should
   * be called prior to this method to distinguish 'missing' columns from columns without indexes.
   */
  @Nullable
  <T> T as(String column, Class<T> clazz);
}
