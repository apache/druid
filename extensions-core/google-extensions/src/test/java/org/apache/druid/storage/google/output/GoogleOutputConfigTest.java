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

package org.apache.druid.storage.google.output;


import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

public class GoogleOutputConfigTest
{

  @TempDir
  public File temporaryFolder;

  private static final String BUCKET = "bucket";
  private static final String PREFIX = "prefix";
  private static final int MAX_RETRY_COUNT = 0;

  @Test
  public void testTooLargeChunkSize()
  {
    HumanReadableBytes chunkSize = new HumanReadableBytes("17MiB");
    Assertions.assertThrows(
        DruidException.class,
        () -> new GoogleOutputConfig(BUCKET, PREFIX, newFolder(temporaryFolder, "junit"), chunkSize, MAX_RETRY_COUNT)
    );
  }

  private static File newFolder(File root, String... subDirs)
  {
    return FileUtils.createTempDirInLocation(root.toPath(), String.join("-", subDirs));
  }
}
