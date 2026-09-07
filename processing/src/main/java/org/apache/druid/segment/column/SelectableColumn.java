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

import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.nested.NestedColumnIndexSupplier;
import org.apache.druid.segment.nested.NestedColumnSelectorFactory;
import org.apache.druid.segment.nested.NestedColumnTypeInspector;
import org.apache.druid.segment.nested.NestedVectorColumnSelectorFactory;

import javax.annotation.Nullable;

/**
 * Class returned by {@link ColumnHolder}. Represents a physical or virtual column that can potentially be used
 * in specialized ways.
 *
 * Currently, this class has no ability to directly make selectors, so it doesn't provide a way to unify
 * e.g. {@link VirtualColumn#makeColumnValueSelector} and {@link BaseColumn#makeColumnValueSelector}. This may
 * be added in the future, but for now it happens as part of the creation of a {@link Cursor}.
 */
public interface SelectableColumn
{
  /**
   * Request an implementation of a particular interface.
   *
   * Physical nested columns, and virtual columns that act as a nested column, should implement:
   * <ul>
   *   <li> {@link NestedColumnTypeInspector}, required.</li>
   *   <li> {@link NestedColumnSelectorFactory}, required.</li>
   *   <li> {@link NestedVectorColumnSelectorFactory}, required.</li>
   *   <li> {@link NestedColumnIndexSupplier}, if the column has indexes.</li>
   * </ul>
   *
   * @param clazz desired interface
   * @param <T>   desired interface
   *
   * @return instance of clazz, or null if the interface is not supported by this segment
   */
  @Nullable
  default <T> T as(Class<T> clazz)
  {
    return null;
  }
}
