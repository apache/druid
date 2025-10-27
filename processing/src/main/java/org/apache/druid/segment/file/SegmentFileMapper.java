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

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Provides access to the internal files of a segment file. Segment files contain multiple internal files stored in the
 * same physical file to more efficiently use file descriptors. At a higher level, columns are composed of one or more
 * of these internal files, and coupled with metadata (also contained within the segment) can be turned into a
 * {@link org.apache.druid.segment.QueryableIndex}.
 *
 * @see org.apache.druid.segment.IndexIO#loadIndex(File)
 * @see SegmentFileBuilder
 */
public interface SegmentFileMapper extends Closeable
{
  /**
   * Returns the list of files contained within in the segment file which can be retrieved with {@link #mapFile(String)}
   */
  Set<String> getInternalFilenames();

  /**
   * Returns a mapped {@link ByteBuffer} which of the contents of the internal file, or null if the file is not
   * present in this segment. The file data is from 0 to {@link ByteBuffer#capacity()}, {@link ByteBuffer#limit()} is
   * equal to the capacity.
   */
  @Nullable
  ByteBuffer mapFile(String name) throws IOException;

  @Override
  void close();
}
