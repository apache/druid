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

package org.apache.druid.storage.azure;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class AzureTestUtils
{
  public static File createZipTempFile(final String segmentFileName, final String content) throws IOException
  {
    final File zipFile = Files.createTempFile("index", ".zip").toFile();
    final byte[] value = content.getBytes("utf8");

    try (ZipOutputStream zipStream = new ZipOutputStream(new FileOutputStream(zipFile))) {
      zipStream.putNextEntry(new ZipEntry(segmentFileName));
      zipStream.write(value);
    }

    return zipFile;
  }
}
