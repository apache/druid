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
package io.druid.segment.store;

import com.google.common.primitives.Ints;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 *
 */
public class DirectoryUtils
{
  /**
   * utils for short length files such as descriptor ones.
   *
   * @param directory
   * @param fileName
   *
   * @return
   *
   * @throws IOException
   */
  public static byte[] openShortFilesAsBytesArrary(Directory directory, String fileName) throws IOException
  {
    byte[] bytes = null;
    try (IndexInput vindexInput = directory.openInput(fileName)) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      WritableByteChannel writableByteChannel = Channels.newChannel(byteArrayOutputStream);
      IndexInputUtils.write2Channel(vindexInput, writableByteChannel);
      bytes = byteArrayOutputStream.toByteArray();
    }
    return bytes;
  }

  /**
   * whether the directory has a file named fileName
   *
   * @param directory
   * @param fileName
   *
   * @return
   *
   * @throws IOException
   */
  public static boolean existFile(Directory directory, String fileName) throws IOException
  {
    String[] fileNames = directory.listAll();
    for (String existFileName : fileNames) {
      if (existFileName.equals(fileName)) {
        return true;
      }
    }
    return false;
  }

  public static boolean hasFileEndWith(Directory directory, String suffix) throws IOException
  {
    String[] fileNames = directory.listAll();
    for (String existFileName : fileNames) {
      if (existFileName.endsWith(suffix)) {
        return true;
      }
    }
    return false;
  }

  public static boolean hasFileStartWith(Directory directory, String prefix) throws IOException
  {
    String[] fileNames = directory.listAll();
    for (String existFileName : fileNames) {
      if (existFileName.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }


  public static int getSegmentVersionFromDir(Directory directory) throws IOException
  {
    boolean existVersionFile = DirectoryUtils.existFile(directory, "version.bin");
    if (existVersionFile) {
      byte[] versionBytes = DirectoryUtils.openShortFilesAsBytesArrary(directory, "version.bin");
      return Ints.fromByteArray(versionBytes);
    }

    int version;
    try (IndexInput indexInput = directory.openInput("index.drd")) {
      version = indexInput.readByte();
    }

    return version;
  }
}
