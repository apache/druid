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

import javax.annotation.Nullable;

/**
 * Interface for methods describing physical segments such as {@link QueryableIndexSegment} and
 * {@link IncrementalIndexSegment} that is not typically used at query time (outside of metadata queries).
 */
public interface PhysicalSegmentInspector extends ColumnInspector
{
  /**
   * Returns {@link Metadata} which contains details about how the segment was created
   */
  @Nullable
  Metadata getMetadata();
  /**
   * Returns the minimum value of the provided column, if known through an index, dictionary, or cache. Returns null
   * if not known. Does not scan the column to find the minimum value.
   */
  @Nullable
  Comparable getMinValue(String column);

  /**
   * Returns the minimum value of the provided column, if known through an index, dictionary, or cache. Returns null
   * if not known. Does not scan the column to find the maximum value.
   */
  @Nullable
  Comparable getMaxValue(String column);

  /**
   * Returns the number of distinct values in a column, if known, or
   * {@link DimensionDictionarySelector#CARDINALITY_UNKNOWN} if not.}
   */
  int getDimensionCardinality(String column);

  /**
   * Returns the number of rows in the segment
   */
  int getNumRows();
}
