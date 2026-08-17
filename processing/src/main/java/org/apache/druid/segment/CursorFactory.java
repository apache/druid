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

import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

public interface CursorFactory extends ColumnInspector
{
  /**
   * Creates a {@link CursorHolder} for a given {@link CursorBuildSpec} which describes how the reader is going to
   * scan, filter, transform, group and aggregate, and/or order the data.
   */
  CursorHolder makeCursorHolder(CursorBuildSpec spec);

  /**
   * Asynchronous variant of {@link #makeCursorHolder(CursorBuildSpec)} for cursor factories that may need to do I/O
   * (e.g. download column data from deep storage) before they can serve a cursor. Callers running on threads that
   * must not block use this rather than {@link #makeCursorHolder}.
   * <p>
   * There is intentionally no working default: this method must be explicitly implemented to participate in
   * async-aware engines (MSQ). A factory whose source is always fully-resident and never needs to block while waiting
   * on some other thread to perform work can implement {@link ResidentCursorFactory} instead of {@link CursorFactory}
   * directly, which provides a default implementation of this method that wraps
   * {@link #makeCursorHolder(CursorBuildSpec)}.
   */
  default AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
  {
    throw DruidException.defensive(
        "makeCursorHolderAsync is not implemented by [%s]. Override it (or implement ResidentCursorFactory): return "
        + "AsyncCursorHolder.completed(makeCursorHolder(spec)) if the source is always fully resident, or build/await "
        + "the cursor holder asynchronously if it supports load on demand.",
        getClass().getName()
    );
  }

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
