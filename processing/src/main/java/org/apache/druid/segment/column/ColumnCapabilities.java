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

/**
 */
public interface ColumnCapabilities
{
  ValueType getType();

  boolean isDictionaryEncoded();
  boolean isRunLengthEncoded();
  boolean hasBitmapIndexes();
  boolean hasSpatialIndexes();
  boolean hasMultipleValues();
  boolean isFilterable();

  /**
   * This property indicates that this {@link ColumnCapabilities} is "complete" in that all properties can be expected
   * to supply valid responses. Not all {@link ColumnCapabilities} are created equal. Some, such as those provided by
   * {@link org.apache.druid.query.groupby.RowBasedColumnSelectorFactory} only have type information, if even that, and
   * cannot supply information like {@link ColumnCapabilities#hasMultipleValues}, and will report as false.
   */
  boolean isComplete();
}
