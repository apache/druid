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

import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;

/**
 * Vectorized object selector.
 *
 * Typically created by {@link VectorColumnSelectorFactory#makeObjectSelector(String)}.
 *
 * @see ColumnValueSelector, the non-vectorized version.
 */
public interface VectorObjectSelector extends VectorSizeInspector
{
  /**
   * Get the current vector.
   *
   * The type of objects in the array depends on the type of the selector. Callers can determine this by calling
   * {@link VectorColumnSelectorFactory#getColumnCapabilities(String)} if creating selectors directly. Alternatively,
   * callers using {@link VectorColumnProcessorFactory} will receive capabilities as part of the callback to
   * {@link VectorColumnProcessorFactory#makeObjectProcessor(ColumnCapabilities, VectorObjectSelector)}.
   *
   * String selectors, where type is {@link ColumnType#STRING}, must use objects compatible with the spec of
   * {@link org.apache.druid.segment.DimensionSelector#rowToObject(IndexedInts, DimensionDictionarySelector)}.
   *
   * Array selectors, where {@link ColumnType#isArray()}, must use {@code Object[]}. The array may contain
   * null elements, and the array itself may also be null.
   *
   * Complex selectors may use any type of object.
   *
   * No other type of selector is possible. Vector object selectors are only used for strings, arrays, and complex types.
   */
  Object[] getObjectVector();
}
