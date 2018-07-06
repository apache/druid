/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.io.smoosh;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import io.druid.java.util.common.ByteBufferUtils;
import io.druid.java.util.common.ISE;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class that works in conjunction with FileSmoosher.  This class knows how to map in a set of files smooshed
 * by the FileSmoosher.
 */
public class SmooshedFileMapper implements Closeable
{
  public static SmooshedFileMapper load(File baseDir) throws IOException
  {
    File metaFile = FileSmoosher.metaFile(baseDir);

    BufferedReader in = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(metaFile), Charsets.UTF_8));

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

      Map<String, Metadata> internalFiles = Maps.newTreeMap();
      while ((line = in.readLine()) != null) {
        splits = line.split(",");

        if (splits.length != 4) {
          throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
        }
        internalFiles.put(
            splits[0],
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
  private final Map<String, Metadata> internalFiles;
  private final List<MappedByteBuffer> buffersList = Lists.newArrayList();

  private SmooshedFileMapper(
      List<File> outFiles,
      Map<String, Metadata> internalFiles
  )
  {
    this.outFiles = outFiles;
    this.internalFiles = internalFiles;
  }

  public Set<String> getInternalFilenames()
  {
    return internalFiles.keySet();
  }

  /**
   * Returns a mapped buffer of the smooshed file with the given name. Buffer's contents from 0 to capacity() are the
   * whole mapped file contents, limit() is equal to capacity().
   */
  public ByteBuffer mapFile(String name) throws IOException
  {
    final Metadata metadata = internalFiles.get(name);
    if (metadata == null) {
      return null;
    }

    final int fileNum = metadata.getFileNum();
    while (buffersList.size() <= fileNum) {
      buffersList.add(null);
    }

    MappedByteBuffer mappedBuffer = buffersList.get(fileNum);
    if (mappedBuffer == null) {
      mappedBuffer = Files.map(outFiles.get(fileNum));
      buffersList.set(fileNum, mappedBuffer);
    }

    ByteBuffer retVal = mappedBuffer.duplicate();
    retVal.position(metadata.getStartOffset()).limit(metadata.getEndOffset());
    return retVal.slice();
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
