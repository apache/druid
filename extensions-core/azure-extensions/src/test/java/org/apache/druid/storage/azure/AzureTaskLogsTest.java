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

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class AzureTaskLogsTest extends EasyMockSupport
{

  private static final String CONTAINER = "test";
  private static final String PREFIX = "test/log";
  private static final String TASK_ID = "taskid";
  private static final String TASK_ID_NOT_FOUND = "taskidNotFound";
  private static final AzureTaskLogsConfig AZURE_TASK_LOGS_CONFIG = new AzureTaskLogsConfig(CONTAINER, PREFIX, 3);

  private AzureStorage azureStorage;
  private AzureTaskLogs azureTaskLogs;

  @Before
  public void before()
  {
    azureStorage = createMock(AzureStorage.class);
    azureTaskLogs = new AzureTaskLogs(AZURE_TASK_LOGS_CONFIG, azureStorage);
  }


  @Test
  public void test_PushTaskLog_uploadsBlob() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File logFile = new File(tmpDir, "log");

      azureStorage.uploadBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/log");
      EasyMock.expectLastCall();

      replayAll();

      azureTaskLogs.pushTaskLog(TASK_ID, logFile);

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test(expected = RuntimeException.class)
  public void test_PushTaskLog_exception_rethrowsException() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File logFile = new File(tmpDir, "log");

      azureStorage.uploadBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/log");
      EasyMock.expectLastCall().andThrow(new IOException());

      replayAll();

      azureTaskLogs.pushTaskLog(TASK_ID, logFile);

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void test_PushTaskReports_uploadsBlob() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File logFile = new File(tmpDir, "log");

      azureStorage.uploadBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/report.json");
      EasyMock.expectLastCall();

      replayAll();

      azureTaskLogs.pushTaskReports(TASK_ID, logFile);

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test(expected = RuntimeException.class)
  public void test_PushTaskReports_exception_rethrowsException() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File logFile = new File(tmpDir, "log");

      azureStorage.uploadBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/report.json");
      EasyMock.expectLastCall().andThrow(new IOException());

      replayAll();

      azureTaskLogs.pushTaskReports(TASK_ID, logFile);

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

    final String blobPath = PREFIX + "/" + TASK_ID + "/log";
    EasyMock.expect(azureStorage.getBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(testLog.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskLog(TASK_ID, 0);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog);

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithPositiveOffset() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/log";
    EasyMock.expect(azureStorage.getBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(testLog.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskLog(TASK_ID, 5);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog.substring(5));

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithNegative() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/log";
    EasyMock.expect(azureStorage.getBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(StringUtils.toUtf8(testLog)));


    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskLog(TASK_ID, -3);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog.substring(testLog.length() - 3));

    verifyAll();
  }

  @Test
  public void test_streamTaskReports_blobExists_succeeds() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/report.json";
    EasyMock.expect(azureStorage.getBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(testLog.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskReports(TASK_ID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog);

    verifyAll();
  }

  @Test
  public void test_streamTaskReports_blobDoesNotExist_returnsAbsent() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID_NOT_FOUND + "/report.json";
    EasyMock.expect(azureStorage.getBlobExists(CONTAINER, blobPath)).andReturn(false);

    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskReports(TASK_ID_NOT_FOUND);


    Assert.assertFalse(byteSource.isPresent());

    verifyAll();
  }

  @Test(expected = IOException.class)
  public void test_streamTaskReports_exceptionWhenGettingStream_throwsException() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/report.json";
    EasyMock.expect(azureStorage.getBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER, blobPath)).andThrow(
        new URISyntaxException("", ""));


    replayAll();

    final Optional<ByteSource> byteSource = azureTaskLogs.streamTaskReports(TASK_ID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    verifyAll();
  }

  @Test(expected = IOException.class)
  public void test_streamTaskReports_exceptionWhenCheckingBlobExistence_throwsException() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/report.json";
    EasyMock.expect(azureStorage.getBlobExists(CONTAINER, blobPath)).andThrow(new URISyntaxException("", ""));

    replayAll();

    azureTaskLogs.streamTaskReports(TASK_ID);

    verifyAll();
  }

  @Test (expected = UnsupportedOperationException.class)
  public void test_killAll_throwsUnsupportedOperationException()
  {
    azureTaskLogs.killAll();
  }

  @Test (expected = UnsupportedOperationException.class)
  public void test_killOlderThan_throwsUnsupportedOperationException()
  {
    azureTaskLogs.killOlderThan(0);
  }

  @After
  public void cleanup()
  {
    resetAll();
  }
}
