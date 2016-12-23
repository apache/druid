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

package io.druid.segment.smooth;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import io.druid.java.util.common.ByteBufferUtils;
import io.druid.java.util.common.ISE;
import io.druid.segment.store.Directory;
import io.druid.segment.store.IndexInput;
import io.druid.segment.store.IndexInputInputStream;

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

  /**
   * smoosh directory contains xxx.meta xxx.smooth ... files
   *
   * @param directory
   *
   * @return
   *
   * @throws IOException
   */
  public static SmooshedFileMapper load(Directory directory) throws IOException
  {
    File baseDir = new File(".");
    File metaFile = FileSmoosher.metaFile(baseDir);
    String metaFileName = metaFile.getName();
    IndexInput indexInput = directory.openInput(metaFileName);
    IndexInputInputStream indexStream = new IndexInputInputStream(indexInput);
    BufferedReader in = null;
    try {
      in = new BufferedReader(new InputStreamReader(indexStream, Charsets.UTF_8));

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

      return new SmooshedFileMapper(outFiles, internalFiles, directory);
    }
    finally {
      Closeables.close(in, false);
    }
  }


  private final List<File> outFiles;
  private final Map<String, Metadata> internalFiles;
  private final List<MappedByteBuffer> buffersList = Lists.newArrayList();
  private final List<IndexInput> indexInputList = Lists.newArrayList();
  private final Directory directory;
  private final boolean isDirectoryVersion;

  private SmooshedFileMapper(
      List<File> outFiles,
      Map<String, Metadata> internalFiles
  )
  {
    this.outFiles = outFiles;
    this.internalFiles = internalFiles;
    this.directory = null;
    this.isDirectoryVersion = false;
  }

  private SmooshedFileMapper(
      List<File> outFiles,
      Map<String, Metadata> internalFiles, Directory directory
  )
  {
    this.outFiles = outFiles;
    this.internalFiles = internalFiles;
    this.directory = directory;
    this.isDirectoryVersion = true;
  }

  public Set<String> getInternalFilenames()
  {
    return internalFiles.keySet();
  }

  public ByteBuffer mapFile(String name) throws IOException
  {
    if(isDirectoryVersion){
      throw new RuntimeException("this smoosh file is not created using ByteBuffer interface!");
    }
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


  public IndexInput openFile(String name) throws IOException
  {
    if(!isDirectoryVersion){
      throw new RuntimeException("this smoosh file is not created using Directory interface!");
    }
    final Metadata metadata = internalFiles.get(name);
    if (metadata == null) {
      return null;
    }

    final int fileNum = metadata.getFileNum();
    while (indexInputList.size() <= fileNum) {
      indexInputList.add(null);
    }

    IndexInput indexInput = indexInputList.get(fileNum);
    if (indexInput == null) {
      File smoothFile = outFiles.get(fileNum);
      String fileName = smoothFile.getName();
      indexInput = directory.openInput(fileName);
      indexInputList.set(fileNum, indexInput);
    }
    IndexInput duplicate = indexInput.duplicate();
    long startOffset = metadata.getStartOffset();
    long endOffset = metadata.getEndOffset();
    long length = endOffset - startOffset;
    IndexInput sliced = duplicate.slice(startOffset, length);
    return sliced;
  }

  @Override
  public void close()
  {
    Throwable thrown = null;
    for (MappedByteBuffer mappedByteBuffer : buffersList) {
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
    Throwables.propagateIfPossible(thrown);
  }

  private void indexInputClose()
  {

  }
}
