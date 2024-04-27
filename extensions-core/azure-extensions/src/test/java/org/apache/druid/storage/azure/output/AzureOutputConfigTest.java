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

package org.apache.druid.storage.azure.output;


import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class AzureOutputConfigTest
{

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String CONTAINER = "container";
  private static final String PREFIX = "prefix";
  private static final int MAX_RETRY_COUNT = 0;

  @Test
  public void testTooLargeChunkSize()
  {
    HumanReadableBytes chunkSize = new HumanReadableBytes("4001MiB");
    Assert.assertThrows(
        DruidException.class,
        () -> new AzureOutputConfig(CONTAINER, PREFIX, temporaryFolder.newFolder(), chunkSize, MAX_RETRY_COUNT)
    );
  }

  @Test
  public void testTempDirectoryNotWritable() throws IOException
  {
    File tempDir = temporaryFolder.newFolder();
    if (!tempDir.setWritable(false)) {
      throw new ISE("Unable to change the permission of temp folder for %s", this.getClass().getName());
    }
    //noinspection ResultOfObjectAllocationIgnored
    Assert.assertThrows(
        DruidException.class,
        () -> new AzureOutputConfig(CONTAINER, PREFIX, tempDir, null, MAX_RETRY_COUNT)
    );
  }

  @Test
  public void testTempDirectoryNotPresentButWritable() throws IOException
  {
    File tempDir = new File(temporaryFolder.newFolder() + "/notPresent1/notPresent2/notPresent3");
    //noinspection ResultOfObjectAllocationIgnored
    new AzureOutputConfig(CONTAINER, PREFIX, tempDir, null, MAX_RETRY_COUNT);
  }

  @Test
  public void testTempDirectoryPresent() throws IOException
  {
    File tempDir = new File(temporaryFolder.newFolder() + "/notPresent1/notPresent2/notPresent3");
    FileUtils.mkdirp(tempDir);
    //noinspection ResultOfObjectAllocationIgnored
    new AzureOutputConfig(CONTAINER, PREFIX, tempDir, null, MAX_RETRY_COUNT);
  }
}
