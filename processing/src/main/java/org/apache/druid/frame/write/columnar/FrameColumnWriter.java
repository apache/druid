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
 * Represent writers for the columnar frames.
 *
 * The class objects must be provided with information on what values to write, usually provided as a
 * {@link org.apache.druid.segment.ColumnValueSelector} and where to write to, usually temporary growable memory
 * {@link #addSelection()} will be called repeatedly, as the current value to write gets updated. For the final write,
 * call {@link #writeTo}, which will copy the values we have added so far to the destination memory.
 */
public interface FrameColumnWriter extends Closeable
{
  /**
   * Adds the current value to the writer
   */
  boolean addSelection();

  /**
   * Reverts the last added value. Undo calls cannot be called in successsion
   */
  void undo();

  /**
   * Size (in bytes) of the column data that will get written when {@link #writeTo} will be called
   */
  long size();

  /**
   * Writes the value of the column to the provided memory at the given position
   */
  long writeTo(WritableMemory memory, long position);

  @Override
  void close();
}
