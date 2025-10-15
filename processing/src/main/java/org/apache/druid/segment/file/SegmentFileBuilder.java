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

package org.apache.druid.segment.file;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Builder for segment file(s) which are created on {@link #close()} and later can be read with a
 * {@link SegmentFileMapper}. At the lowest level, segment files are built by packing multiple chunks of data organized
 * as internal files, into the same physical file to efficiently share file descriptors, At a higher level, Druid
 * columns are composed of one or more files containing the column parts, which are stored in the segment during
 * serialization using this builder.
 *
 * @see org.apache.druid.segment.IndexMergerV9
 * @see org.apache.druid.segment.serde.Serializer
 * @see org.apache.druid.segment.column.ColumnDescriptor
 * @see SegmentFileMapper
 */
public interface SegmentFileBuilder extends Closeable
{
  /**
   * Add a {@link File} to the segment file as the specified name
   */
  void add(String name, File fileToAdd) throws IOException;

  /**
   * Add a {@link ByteBuffer} to a segment file as the specified name
   */
  void add(String name, ByteBuffer bufferToAdd) throws IOException;

  /**
   * Creates a {@link SegmentFileChannel} to write data to the segment file as the specified name. Callers must be sure
   * to write the amount of data specified by the size parameter.
   */
  SegmentFileChannel addWithChannel(String name, long size) throws IOException;
}
