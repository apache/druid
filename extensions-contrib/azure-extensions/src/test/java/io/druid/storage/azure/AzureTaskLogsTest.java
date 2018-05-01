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

package io.druid.storage.azure;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import io.druid.java.util.common.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

public class AzureTaskLogsTest extends EasyMockSupport
{

  private static final String container = "test";
  private static final String prefix = "test/log";
  private static final String taskid = "taskid";
  private static final AzureTaskLogsConfig azureTaskLogsConfig = new AzureTaskLogsConfig(container, prefix, 3);

  private AzureStorage azureStorage;
  private AzureTaskLogs azureTaskLogs;

  @Before
  public void before()
  {
    azureStorage = createMock(AzureStorage.class);
    azureTaskLogs = new AzureTaskLogs(azureTaskLogsConfig, azureStorage);
  }


  @Test
  public void testPushTaskLog() throws Exception
  {
    final File tmpDir = Files.createTempDir();

    try {
      final File logFile = new File(tmpDir, "log");

      azureStorage.uploadBlob(logFile, container, prefix + "/" + taskid + "/log");
      expectLastCall();

      replayAll();

      azureTaskLogs.pushTaskLog(taskid, logFile);

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void testStreamTaskLogWithoutOffset() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = prefix + "/" + taskid + "/log";
    expect(azureStorage.getBlobExists(container, blobPath)).andReturn(true);
    expect(azureStorage.getBlobLength(container, blobPath)).andReturn((long) testLog.length());
    expect(azureStorage.getBlobInputStream(container, blobPath)).andReturn(
        new ByteArrayInputStream(testLog.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskLog(taskid, 0);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog);

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithPositiveOffset() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = prefix + "/" + taskid + "/log";
    expect(azureStorage.getBlobExists(container, blobPath)).andReturn(true);
    expect(azureStorage.getBlobLength(container, blobPath)).andReturn((long) testLog.length());
    expect(azureStorage.getBlobInputStream(container, blobPath)).andReturn(
        new ByteArrayInputStream(testLog.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskLog(taskid, 5);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog.substring(5));

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithNegative() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = prefix + "/" + taskid + "/log";
    expect(azureStorage.getBlobExists(container, blobPath)).andReturn(true);
    expect(azureStorage.getBlobLength(container, blobPath)).andReturn((long) testLog.length());
    expect(azureStorage.getBlobInputStream(container, blobPath)).andReturn(
        new ByteArrayInputStream(StringUtils.toUtf8(testLog)));


    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskLog(taskid, -3);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog.substring(testLog.length() - 3));

    verifyAll();
  }
}
