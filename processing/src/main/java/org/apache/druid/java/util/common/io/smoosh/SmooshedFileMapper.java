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

package org.apache.druid.java.util.common.io.smoosh;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.file.SegmentFileMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Class that works in conjunction with FileSmoosher.  This class knows how to map in a set of files smooshed
 * by the FileSmoosher.
 */
public class SmooshedFileMapper implements SegmentFileMapper
{
  /**
   * Interner for smoosh internal files, which includes all column names since every column has an internal file
   * associated with it
   */
  public static final Interner<String> STRING_INTERNER = Interners.newWeakInterner();

  /**
   * Number of ints per file in {@link #internalFileLocations}, and the slot of each value within those ints.
   */
  private static final int INTS_PER_LOCATION = 3;
  private static final int FILE_NUM_SLOT = 0;
  private static final int START_OFFSET_SLOT = 1;
  private static final int END_OFFSET_SLOT = 2;

  public static SmooshedFileMapper load(File baseDir) throws IOException
  {
    File metaFile = FileSmoosher.metaFile(baseDir);

    BufferedReader in = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(metaFile), StandardCharsets.UTF_8));

      String line = in.readLine();
      if (line == null) {
        throw new ISE("First line should be version,maxChunkSize,numChunks, got null.");
      }

      String[] splits = line.split(",");
      if (!"v1".equals(splits[0])) {
        throw new ISE("Unknown version[%s], v1 is all I know.", splits[0]);
      }
      if (splits.length != 3) {
        throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
      }
      final Integer numFiles = Integer.valueOf(splits[2]);
      List<File> outFiles = Lists.newArrayListWithExpectedSize(numFiles);

      for (int i = 0; i < numFiles; ++i) {
        outFiles.add(FileSmoosher.makeChunkFile(baseDir, i));
      }

      SortedMap<String, Metadata> internalFiles = new TreeMap<>();
      while ((line = in.readLine()) != null) {
        splits = line.split(",");

        if (splits.length != 4) {
          throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
        }
        internalFiles.put(
            STRING_INTERNER.intern(splits[0]),
            new Metadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3]))
        );
      }

      return new SmooshedFileMapper(outFiles, internalFiles);
    }
    finally {
      Closeables.close(in, false);
    }
  }

  private final List<File> outFiles;

  /**
   * Names of internal files, sorted. This is used instead of a map to reduce the footprint of each loaded segment.
   */
  private final String[] internalFileNames;

  /**
   * Location of each file in {@link #internalFileNames}, as a group of {@link #INTS_PER_LOCATION} ints: chunk number,
   * start offset, and end offset. Read with {@link #getFileNum}, {@link #getStartOffset}, and {@link #getEndOffset}.
   */
  private final int[] internalFileLocations;

  private final List<MappedByteBuffer> buffersList = new ArrayList<>();

  private SmooshedFileMapper(
      List<File> outFiles,
      SortedMap<String, Metadata> internalFiles
  )
  {
    this.outFiles = outFiles;
    this.internalFileNames = new String[internalFiles.size()];
    this.internalFileLocations = new int[internalFiles.size() * INTS_PER_LOCATION];

    int i = 0;
    for (Map.Entry<String, Metadata> entry : internalFiles.entrySet()) {
      internalFileNames[i] = entry.getKey();
      final int location = i * INTS_PER_LOCATION;
      internalFileLocations[location + FILE_NUM_SLOT] = entry.getValue().getFileNum();
      internalFileLocations[location + START_OFFSET_SLOT] = entry.getValue().getStartOffset();
      internalFileLocations[location + END_OFFSET_SLOT] = entry.getValue().getEndOffset();
      i++;
    }
  }

  @Override
  public Set<String> getInternalFilenames()
  {
    return new AbstractSet<>()
    {
      @Override
      public Iterator<String> iterator()
      {
        return Arrays.asList(internalFileNames).iterator();
      }

      @Override
      public int size()
      {
        return internalFileNames.length;
      }

      @Override
      public boolean contains(Object o)
      {
        return o instanceof String && Arrays.binarySearch(internalFileNames, o) >= 0;
      }
    };
  }

  @Override
  public ByteBuffer mapFile(String name) throws IOException
  {
    final int index = Arrays.binarySearch(internalFileNames, name);
    if (index < 0) {
      return null;
    }

    final int fileNum = getFileNum(index);
    while (buffersList.size() <= fileNum) {
      buffersList.add(null);
    }

    MappedByteBuffer mappedBuffer = buffersList.get(fileNum);
    if (mappedBuffer == null) {
      mappedBuffer = Files.map(outFiles.get(fileNum));
      buffersList.set(fileNum, mappedBuffer);
    }

    ByteBuffer retVal = mappedBuffer.duplicate();
    retVal.position(getStartOffset(index)).limit(getEndOffset(index));
    return retVal.slice();
  }

  /**
   * Returns the chunk number of the file at the given position of {@link #internalFileNames}.
   */
  private int getFileNum(int index)
  {
    return internalFileLocations[index * INTS_PER_LOCATION + FILE_NUM_SLOT];
  }

  /**
   * Returns the start offset, within its chunk, of the file at the given position of {@link #internalFileNames}.
   */
  private int getStartOffset(int index)
  {
    return internalFileLocations[index * INTS_PER_LOCATION + START_OFFSET_SLOT];
  }

  /**
   * Returns the end offset, within its chunk, of the file at the given position of {@link #internalFileNames}.
   */
  private int getEndOffset(int index)
  {
    return internalFileLocations[index * INTS_PER_LOCATION + END_OFFSET_SLOT];
  }

  @Override
  public void close()
  {
    Throwable thrown = null;
    for (MappedByteBuffer mappedByteBuffer : buffersList) {
      if (mappedByteBuffer == null) {
        continue;
      }
      try {
        ByteBufferUtils.unmap(mappedByteBuffer);
      }
      catch (Throwable t) {
        if (thrown == null) {
          thrown = t;
        } else {
          thrown.addSuppressed(t);
        }
      }
    }
    buffersList.clear();
    if (thrown != null) {
      throw new RuntimeException(thrown);
    }
  }
}
