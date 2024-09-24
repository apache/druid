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

import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;

/**
 * Vectorized cursor used during query execution. VectorCursors are available from
 * {@link CursorFactory#makeCursorHolder(CursorBuildSpec)} via
 * {@link org.apache.druid.segment.CursorHolder#asVectorCursor()}.
 * <p>
 * See {@link org.apache.druid.query.vector.VectorCursorGranularizer} for a helper that makes it easier for query
 * engines to do time granularization.
 * <p>
 * An example of how to use the methods in this class:
 *
 * <pre>
 *   try (CursorHolder cursorHolder = adapter.makeCursorHolder(...)) {
 *     VectorCursor cursor = cursorHolder.asVectorCursor();
 *     // ProcessorClass is some vectorized processor class.
 *     ProcessorClass o = makeProcessor(cursor.getColumnSelectorFactory());
 *     for (; !cursor.isDone(); cursor.advance()) {
 *       o.process();
 *     }
 *   }
 * </pre>
 *
 * @see org.apache.druid.segment.Cursor, the non-vectorized version.
 */
public interface VectorCursor extends VectorSizeInspector
{
  /**
   * Returns a vectorized column selector factory.
   */
  VectorColumnSelectorFactory getColumnSelectorFactory();

  /**
   * Advances the cursor, skipping forward a number of rows equal to the current vector size.
   */
  void advance();

  /**
   * Returns false if the cursor is readable, true if it has nothing left to read.
   */
  boolean isDone();

  /**
   * Resets the cursor back to its original state. Useful for query engines that want to make multiple passes.
   */
  @SuppressWarnings("unused") /* Not currently used, but anticipated to be used by topN in the future. */
  void reset();
}
