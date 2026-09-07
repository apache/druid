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

package org.apache.druid.segment.loading;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provides byte-range read access to segment files in deep storage. This is the extension point for supporting
 * partial segment downloads from different storage backends (S3, local, GCS, HDFS, etc.).
 * <p>
 * The {@code filename} parameter identifies which file to read within the segment's storage location. For the main
 * segment file this is {@link org.apache.druid.segment.IndexIO#V10_FILE_NAME}, and for external segment files it is
 * the external file's name.
 * <p>
 * Implementations must be safe to use from multiple threads, as concurrent queries may trigger range reads for
 * different parts of the same segment file simultaneously.
 *
 * @see LoadSpec#openRangeReader()
 */
public interface SegmentRangeReader
{
  /**
   * Read a contiguous byte range from a file in the segment's storage location.
   *
   * @param filename the name of the file to read, relative to the segment's storage location
   * @param offset   absolute byte offset in the file
   * @param length   maximum number of bytes to read. The returned stream may contain fewer bytes if {@code offset +
   *                 length} exceeds the file size.
   * @return an {@link InputStream} containing up to {@code length} bytes starting at {@code offset}. The caller is
   *         responsible for closing this stream.
   * @throws IOException if the range read fails
   */
  InputStream readRange(String filename, long offset, long length) throws IOException;
}
