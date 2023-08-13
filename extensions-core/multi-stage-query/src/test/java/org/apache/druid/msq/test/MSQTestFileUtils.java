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

package org.apache.druid.msq.test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class MSQTestFileUtils
{

  /**
   * Helper method that copies a resource to a temporary file, then returns it.
   */
  public static File getResourceAsTemporaryFile(TemporaryFolder temporaryFolder, Object object, final String resource) throws IOException
  {
    final File file = temporaryFolder.newFile();
    final InputStream stream = object.getClass().getResourceAsStream(resource);

    if (stream == null) {
      throw new IOE("No such resource [%s]", resource);
    }

    ByteStreams.copy(stream, Files.newOutputStream(file.toPath()));
    return file;
  }

  /**
   * Helper method that populates a temporary file with {@code numRows} rows and {@code numColumns} columns where the
   * first column is a string 'timestamp' while the rest are string columns with junk value
   */
  public static File generateTemporaryNdJsonFile(TemporaryFolder temporaryFolder, final int numRows, final int numColumns) throws IOException
  {
    final File file = temporaryFolder.newFile();
    for (int currentRow = 0; currentRow < numRows; ++currentRow) {
      StringBuilder sb = new StringBuilder();
      sb.append("{");
      sb.append("\"timestamp\":\"2016-06-27T00:00:11.080Z\"");
      for (int currentColumn = 1; currentColumn < numColumns; ++currentColumn) {
        sb.append(StringUtils.format(",\"column%s\":\"val%s\"", currentColumn, currentRow));
      }
      sb.append("}");
      Files.write(file.toPath(), ImmutableList.of(sb.toString()), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }
    file.deleteOnExit();
    return file;
  }
}
