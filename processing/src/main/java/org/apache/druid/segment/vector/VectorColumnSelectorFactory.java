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

package org.apache.druid.segment.vector;

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

/**
 * A class that comes from {@link VectorCursor#getColumnSelectorFactory()} and is used to create vector selectors.
 *
 * If you need to write code that adapts to different input types, you should write a
 * {@link org.apache.druid.segment.VectorColumnProcessorFactory} and use one of the
 * {@link org.apache.druid.segment.ColumnProcessors#makeVectorProcessor} functions instead of using this class.
 *
 * @see org.apache.druid.segment.ColumnSelectorFactory the non-vectorized version.
 */
public interface VectorColumnSelectorFactory extends ColumnInspector
{
  /**
   * Returns a {@link ReadableVectorInspector} for the {@link VectorCursor} that generated this object.
   */
  ReadableVectorInspector getReadableVectorInspector();

  /**
   * Returns the maximum vector size for the {@link VectorCursor} that generated this object.
   *
   * @see VectorCursor#getMaxVectorSize()
   */
  default int getMaxVectorSize()
  {
    return getReadableVectorInspector().getMaxVectorSize();
  }

  /**
   * Returns a string-typed, single-value-per-row column selector. Should only be called on columns where
   * {@link #getColumnCapabilities} indicates they return STRING, or on nonexistent columns.
   *
   * If you need to write code that adapts to different input types, you should write a
   * {@link org.apache.druid.segment.VectorColumnProcessorFactory} and use one of the
   * {@link org.apache.druid.segment.ColumnProcessors#makeVectorProcessor} functions instead of using this method.
   */
  SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec);

  /**
   * Returns a string-typed, multi-value-per-row column selector. Should only be called on columns where
   * {@link #getColumnCapabilities} indicates they return STRING. Unlike {@link #makeSingleValueDimensionSelector},
   * this should not be called on nonexistent columns.
   *
   * If you need to write code that adapts to different input types, you should write a
   * {@link org.apache.druid.segment.VectorColumnProcessorFactory} and use one of the
   * {@link org.apache.druid.segment.ColumnProcessors#makeVectorProcessor} functions instead of using this method.
   */
  MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec);

  /**
   * Returns a primitive column selector. Should only be called on columns where {@link #getColumnCapabilities}
   * indicates they return DOUBLE, FLOAT, or LONG, or on nonexistent columns.
   *
   * If you need to write code that adapts to different input types, you should write a
   * {@link org.apache.druid.segment.VectorColumnProcessorFactory} and use one of the
   * {@link org.apache.druid.segment.ColumnProcessors#makeVectorProcessor} functions instead of using this method.
   */
  VectorValueSelector makeValueSelector(String column);

  /**
   * Returns an object selector. Should only be called on columns where {@link #getColumnCapabilities} indicates that
   * they return STRING or COMPLEX, or on nonexistent columns.
   *
   * If you need to write code that adapts to different input types, you should write a
   * {@link org.apache.druid.segment.VectorColumnProcessorFactory} and use one of the
   * {@link org.apache.druid.segment.ColumnProcessors#makeVectorProcessor} functions instead of using this method.
   */
  VectorObjectSelector makeObjectSelector(String column);

  /**
   * Returns capabilities of a particular column, or null if the column doesn't exist. Unlike ColumnSelectorFactory,
   * null does not potentially indicate a dynamically discovered column.
   *
   * @return capabilities, or null if the column doesn't exist.
   */
  @Override
  @Nullable
  ColumnCapabilities getColumnCapabilities(String column);
}
