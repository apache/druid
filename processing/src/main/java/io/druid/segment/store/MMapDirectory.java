/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.druid.segment.store;


import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 *
 */
public class MMapDirectory extends FSDirectory
{

  /**
   * Create a new MMapDirectory for the named location.
   * The directory is created at the named location if it does not yet exist.
   *
   * @param path the path of the directory
   */
  public MMapDirectory(Path path) throws IOException
  {
    super(path);
  }

  /**
   * Creates an IndexInput for the file with the given name.
   */
  @Override
  public IndexInput openInput(String name) throws IOException
  {
    Path path = directory.resolve(name);
    File file = path.toFile();
    ByteBuffer byteBuffer = Files.map(file);
    ByteBufferIndexInput byteBufferIndexInput = new ByteBufferIndexInput(byteBuffer);
    return byteBufferIndexInput;
  }

  /**
   * open input from position
   * @param name
   * @param position
   * @param size  the size of region to map
   * @return
   * @throws IOException
   */
  public IndexInput openInput(String name, long position, long size) throws IOException
  {
    Path path = directory.resolve(name);
    File file = path.toFile();
    FileChannel fileChannel = new RandomAccessFile(file, FileChannel.MapMode.READ_ONLY.toString()).getChannel();
    ByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size);
    ByteBufferIndexInput byteBufferIndexInput = new ByteBufferIndexInput(byteBuffer);
    return byteBufferIndexInput;
  }


}
