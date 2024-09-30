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

import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

public interface CursorFactory extends ColumnInspector
{
  CursorHolder makeCursorHolder(CursorBuildSpec spec);

  /**
   * Returns the {@link RowSignature} of the data available from this cursor factory. For mutable segments, even though
   * the signature may evolve over time, any particular object returned by this method is an immutable snapshot.
   */
  RowSignature getRowSignature();

  /**
   * Returns capabilities of a particular column, if known. May be null if the column doesn't exist, or if
   * the column does exist but the capabilities are unknown. The latter is possible with dynamically discovered
   * columns.
   *
   * Note that CursorFactory are representations of "real" segments, so they are not aware of any virtual columns
   * that may be involved in a query. In general, query engines should instead use the method
   * {@link ColumnSelectorFactory#getColumnCapabilities(String)}, which returns capabilities for virtual columns as
   * well.
   *
   * @param column column name
   *
   * @return capabilities, or null
   */
  @Override
  @Nullable
  ColumnCapabilities getColumnCapabilities(String column);
}
