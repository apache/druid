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

package org.apache.druid.frame.write.columnar;

import org.apache.datasketches.memory.WritableMemory;

import java.io.Closeable;

/**
 * Subclasses of this interface aid in writing the column values to a column-oriented Frame.
 * The intended implementation of the interface is that the implementations acquire the necessary information and data
 * to write a column (usually a {@link org.apache.druid.segment.ColumnValueSelector}) and write the selection
 * to an internal memory location incrementally as {@link #addSelection()} is called.
 *
 * The {@link #writeTo} call then transfers the build up data from the internal buffer to the provided location, along
 * with some metadata (for e.g. the column type)
 */
public interface FrameColumnWriter extends Closeable
{
  /**
   * Process and add the value from a single row to the internal memory location. This call might fail
   * due to internal errors, processing errors or if we are unable to reserve the internal memory required to
   * make the call.
   * @return true if we can succesfully add the row, false if not
   */
  boolean addSelection();

  /**
   * Undo the last value that was added by {@link #addSelection}. This method helps clean up the last succesful write
   * to the FrameColumnWriter, if the other FrameColumnWriters failed while trying to {@link #addSelection}
   *
   * Calling undo twice in a row, without adding any selection is undefined and may throw an
   * error for some types of FrameColumnWriters like {@link StringFrameColumnWriterImpl}
   */
  void undo();

  /**
   * Memory size required to write the data added so far to memory. This includes the overheads of writing the
   * column types etc.
   */
  long size();

  /**
   * Writes the data to the memory provided at the given location
   */
  long writeTo(WritableMemory memory, long position);

  @Override
  void close();
}
