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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.key.RowKey;

import java.io.Closeable;

/**
 * Helper used to write field values to row-based frames or {@link RowKey}.
 *
 * Most callers should use {@link org.apache.druid.frame.write.FrameWriters} to build frames from
 * {@link org.apache.druid.segment.ColumnSelectorFactory}, rather than using this interface directly.
 *
 * Not thread-safe.
 */
public interface FieldWriter extends Closeable
{
  /**
   * Writes the current selection at the given memory position.
   *
   * @param memory   memory region in little-endian order
   * @param position position to write
   * @param maxSize  maximum number of bytes to write
   *
   * @return number of bytes written, or -1 if "maxSize" was not enough memory
   */
  long writeTo(WritableMemory memory, long position, long maxSize);

  /**
   * Releases resources held by this writer.
   */
  @Override
  void close();
}
