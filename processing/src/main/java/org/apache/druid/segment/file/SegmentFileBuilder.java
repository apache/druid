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

import org.apache.druid.segment.column.ColumnDescriptor;

import javax.annotation.Nullable;
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
 * @see ColumnDescriptor
 * @see SegmentFileMapper
 */
public interface SegmentFileBuilder extends Closeable
{
  /**
   * Add a column to the metadata of this segment file
   */
  void addColumn(String name, ColumnDescriptor columnDescriptor);

  /**
   * Declare that subsequent writes belong to a named group of files that should be stored together. This is a hint
   * about physical layout, it does not constrain the names of files subsequently added, and implementations are free
   * to ignore it entirely (the default is a no-op for formats that don't organize data into coarse-grained
   * groupings). Projections are the primary caller today, but the mechanism is generic, it's equally applicable to
   * grouping internal metadata, data shared across columns, etc.
   * <p>
   * Callers should invoke this before writing each group's files; passing {@code null} clears the current group.
   * Callers should not invoke this while a writer returned by {@link #addWithChannel} is still open (implementations
   * may reject such calls).
   *
   * @see SegmentFileBuilderV10#startFileGroup(String) for the V10 semantics
   */
  default void startFileGroup(@Nullable String groupName)
  {
  }

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

  /**
   * Allow adding data to an 'external' segment container file which will be built in the same directory as this
   * segment file. Legacy implementations of this method just return the same builder since they do not support this
   * concept. This allows column implementations to use these methods, but also still work with older segment formats
   * (assuming the older format otherwise supports the contents).
   */
  default SegmentFileBuilder getExternalBuilder(String externalFile)
  {
    return this;
  }

  /**
   * cleanup any open resources in the event of an exception while building the segment files
   */
  void abort();

  /**
   * Close the segment file builder, writing the file(s) to the destination
   */
  @Override
  void close() throws IOException;
}
