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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * A {@link SegmentRangeReader} backed by a directory of files: each {@code filename} passed to
 * {@link #readRange} resolves to a child of the directory and is read at the requested byte range via
 * {@link RandomAccessFile}.
 */
public class DirectoryBackedRangeReader implements SegmentRangeReader
{
  private final File directory;

  public DirectoryBackedRangeReader(File directory)
  {
    this.directory = directory;
  }

  @Override
  public InputStream readRange(String filename, long offset, long length) throws IOException
  {
    final File target = new File(directory, filename);
    try (RandomAccessFile raf = new RandomAccessFile(target, "r")) {
      final int available = (int) Math.min(length, Math.max(0, raf.length() - offset));
      final byte[] data = new byte[available];
      raf.seek(offset);
      raf.readFully(data);
      return new ByteArrayInputStream(data);
    }
  }
}
