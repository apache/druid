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

package io.druid.utils;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.druid.java.util.common.logger.Logger;

/**
 */
public class CompressionUtils
{
  private static final Logger log = new Logger(CompressionUtils.class);


  @Deprecated // Use com.metamx.common.CompressionUtils.zip
  public static long zip(File directory, File outputZipFile) throws IOException
  {
    return io.druid.java.util.common.CompressionUtils.zip(directory, outputZipFile);
  }


  @Deprecated // Use com.metamx.common.CompressionUtils.zip
  public static long zip(File directory, OutputStream out) throws IOException
  {
    return io.druid.java.util.common.CompressionUtils.zip(directory, out);
  }

  @Deprecated // Use com.metamx.common.CompressionUtils.unzip
  public static void unzip(File pulledFile, File outDir) throws IOException
  {
    io.druid.java.util.common.CompressionUtils.unzip(pulledFile, outDir);
  }

  @Deprecated // Use com.metamx.common.CompressionUtils.unzip
  public static void unzip(InputStream in, File outDir) throws IOException
  {
    io.druid.java.util.common.CompressionUtils.unzip(in, outDir);
  }

  /**
   * Uncompress using a gzip uncompress algorithm from the `pulledFile` to the `outDir`.
   * Unlike `com.metamx.common.CompressionUtils.gunzip`, this function takes an output *DIRECTORY* and tries to guess the file name.
   * It is recommended that the caller use `com.metamx.common.CompressionUtils.gunzip` and specify the output file themselves to ensure names are as expected
   *
   * @param pulledFile The source file
   * @param outDir     The destination directory to put the resulting file
   *
   * @throws IOException on propogated IO exception, IAE if it cannot determine the proper new name for `pulledFile`
   */
  @Deprecated // See description for alternative
  public static void gunzip(File pulledFile, File outDir) throws IOException
  {
    final File outFile = new File(outDir, io.druid.java.util.common.CompressionUtils.getGzBaseName(pulledFile.getName()));
    io.druid.java.util.common.CompressionUtils.gunzip(pulledFile, outFile);
    if (!pulledFile.delete()) {
      log.error("Could not delete tmpFile[%s].", pulledFile);
    }
  }

}
