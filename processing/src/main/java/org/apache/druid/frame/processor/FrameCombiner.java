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

package org.apache.druid.frame.processor;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.segment.ColumnSelectorFactory;

/**
 * Interface for combining adjacent rows with identical sort keys during merge-sort. Works directly with
 * {@link Frame} objects and row indices for efficient random-access reads.
 *
 * Used by {@link FrameChannelMerger} to reduce intermediate data volume by combining rows that share
 * the same sort key.
 */
public interface FrameCombiner
{
  /**
   * Initialize with the FrameReader for understanding frame schema and format.
   */
  void init(FrameReader frameReader);

  /**
   * Start a new group with the given row from the given frame.
   * Reads and stores values from this row.
   */
  void reset(Frame frame, int row);

  /**
   * Fold another row into the accumulated group. Only called when the row
   * has the same sort key as the group started in {@link #reset}.
   */
  void combine(Frame frame, int row);

  /**
   * Returns a {@link ColumnSelectorFactory} backed by the accumulated combined values.
   * Used internally by {@link FrameChannelMerger} for writing combined rows via FrameWriter.
   *
   * For key columns, returns values from the first row of the group.
   * For value columns, returns the result of combining all rows.
   */
  ColumnSelectorFactory getCombinedColumnSelectorFactory();
}
