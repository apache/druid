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

import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.druid.common.utils.CurrentTimeMillisSupplier;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class AzureTaskLogsTest extends EasyMockSupport
{

  private static final String CONTAINER = "test";
  private static final String PREFIX = "test/log";
  private static final String TASK_ID = "taskid";
  private static final String TASK_ID_NOT_FOUND = "taskidNotFound";
  private static final AzureTaskLogsConfig AZURE_TASK_LOGS_CONFIG = new AzureTaskLogsConfig(CONTAINER, PREFIX);
  private static final int MAX_KEYS = 1;
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final long TIME_NOW = 2L;
  private static final long TIME_FUTURE = 3L;
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("azure://%s/%s", CONTAINER, PREFIX));
  private static final int MAX_TRIES = 3;

  private static final Exception NON_RECOVERABLE_EXCEPTION = new BlobStorageException("", null, null);

  private AzureInputDataConfig inputDataConfig;
  private AzureAccountConfig accountConfig;
  private AzureStorage azureStorage;
  private AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;
  private CurrentTimeMillisSupplier timeSupplier;
  private AzureTaskLogs azureTaskLogs;

  @Before
  public void before()
  {
    inputDataConfig = createMock(AzureInputDataConfig.class);
    accountConfig = createMock(AzureAccountConfig.class);
    azureStorage = createMock(AzureStorage.class);
    azureCloudBlobIterableFactory = createMock(AzureCloudBlobIterableFactory.class);
    timeSupplier = createMock(CurrentTimeMillisSupplier.class);
    azureTaskLogs = new AzureTaskLogs(
        AZURE_TASK_LOGS_CONFIG,
        inputDataConfig,
        accountConfig,
        azureStorage,
        azureCloudBlobIterableFactory,
        timeSupplier);
  }


  @Test
  public void test_PushTaskLog_uploadsBlob() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File logFile = new File(tmpDir, "log");

      azureStorage.uploadBlockBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/log", MAX_TRIES);
      EasyMock.expectLastCall();

      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();

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

      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();
      azureStorage.uploadBlockBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/log", MAX_TRIES);
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

      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();
      azureStorage.uploadBlockBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/report.json", MAX_TRIES);
      EasyMock.expectLastCall();

      replayAll();

      azureTaskLogs.pushTaskReports(TASK_ID, logFile);

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void test_PushTaskStatus_uploadsBlob() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File logFile = new File(tmpDir, "status.json");

      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();
      azureStorage.uploadBlockBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/status.json", MAX_TRIES);
      EasyMock.expectLastCall();

      replayAll();

      azureTaskLogs.pushTaskStatus(TASK_ID, logFile);

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void test_PushTaskPayload_uploadsBlob() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File taskFile = new File(tmpDir, "task.json");

      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();
      azureStorage.uploadBlockBlob(taskFile, CONTAINER, PREFIX + "/" + TASK_ID + "/task.json", MAX_TRIES);
      EasyMock.expectLastCall();

      replayAll();

      azureTaskLogs.pushTaskPayload(TASK_ID, taskFile);

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

      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();
      azureStorage.uploadBlockBlob(logFile, CONTAINER, PREFIX + "/" + TASK_ID + "/report.json", MAX_TRIES);
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
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(testLog.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskLog(TASK_ID, 0);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog);

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithPositiveOffset() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/log";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(testLog.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskLog(TASK_ID, 5);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog.substring(5));

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithNegative() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/log";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(StringUtils.toUtf8(testLog)));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskLog(TASK_ID, -3);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog.substring(testLog.length() - 3));

    verifyAll();
  }

  @Test
  public void test_streamTaskReports_blobExists_succeeds() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/report.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(testLog.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskReports(TASK_ID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog);

    verifyAll();
  }

  @Test
  public void test_streamTaskReports_blobDoesNotExist_returnsAbsent() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID_NOT_FOUND + "/report.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(false);

    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskReports(TASK_ID_NOT_FOUND);


    Assert.assertFalse(stream.isPresent());

    verifyAll();
  }

  @Test(expected = IOException.class)
  public void test_streamTaskReports_exceptionWhenGettingStream_throwsException() throws Exception
  {
    final String testLog = "hello this is a log";

    final String blobPath = PREFIX + "/" + TASK_ID + "/report.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) testLog.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andThrow(
        new BlobStorageException("", null, null));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskReports(TASK_ID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    verifyAll();
  }

  @Test(expected = IOException.class)
  public void test_streamTaskReports_exceptionWhenCheckingBlobExistence_throwsException() throws Exception
  {

    final String blobPath = PREFIX + "/" + TASK_ID + "/report.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andThrow(new BlobStorageException("", null, null));

    replayAll();

    azureTaskLogs.streamTaskReports(TASK_ID);

    verifyAll();
  }

  @Test
  public void test_streamTaskStatus_blobExists_succeeds() throws Exception
  {
    final String taskStatus = "{}";

    final String blobPath = PREFIX + "/" + TASK_ID + "/status.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) taskStatus.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(taskStatus.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskStatus(TASK_ID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), taskStatus);

    verifyAll();
  }

  @Test
  public void test_streamTaskStatus_blobDoesNotExist_returnsAbsent() throws Exception
  {
    final String blobPath = PREFIX + "/" + TASK_ID_NOT_FOUND + "/status.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(false);

    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskStatus(TASK_ID_NOT_FOUND);


    Assert.assertFalse(stream.isPresent());

    verifyAll();
  }

  @Test(expected = IOException.class)
  public void test_streamTaskStatus_exceptionWhenGettingStream_throwsException() throws Exception
  {
    final String taskStatus = "{}";

    final String blobPath = PREFIX + "/" + TASK_ID + "/status.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) taskStatus.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andThrow(
        new BlobStorageException("", null, null));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskStatus(TASK_ID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    verifyAll();
  }

  @Test(expected = IOException.class)
  public void test_streamTaskStatus_exceptionWhenCheckingBlobExistence_throwsException() throws Exception
  {
    final String blobPath = PREFIX + "/" + TASK_ID + "/status.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andThrow(new BlobStorageException("", null, null));

    replayAll();

    azureTaskLogs.streamTaskStatus(TASK_ID);

    verifyAll();
  }

  @Test
  public void test_streamTaskPayload_blobExists_succeeds() throws Exception
  {
    final String taskPayload = "{}";

    final String blobPath = PREFIX + "/" + TASK_ID + "/task.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) taskPayload.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andReturn(
        new ByteArrayInputStream(taskPayload.getBytes(StandardCharsets.UTF_8)));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskPayload(TASK_ID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), taskPayload);

    verifyAll();
  }

  @Test
  public void test_streamTaskPayload_blobDoesNotExist_returnsAbsent() throws Exception
  {
    final String blobPath = PREFIX + "/" + TASK_ID_NOT_FOUND + "/task.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(false);

    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskPayload(TASK_ID_NOT_FOUND);


    Assert.assertFalse(stream.isPresent());

    verifyAll();
  }

  @Test(expected = IOException.class)
  public void test_streamTaskPayload_exceptionWhenGettingStream_throwsException() throws Exception
  {
    final String taskPayload = "{}";

    final String blobPath = PREFIX + "/" + TASK_ID + "/task.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andReturn(true);
    EasyMock.expect(azureStorage.getBlockBlobLength(CONTAINER, blobPath)).andReturn((long) taskPayload.length());
    EasyMock.expect(azureStorage.getBlockBlobInputStream(CONTAINER, blobPath)).andThrow(
        new BlobStorageException("", null, null));


    replayAll();

    final Optional<InputStream> stream = azureTaskLogs.streamTaskPayload(TASK_ID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    verifyAll();
  }

  @Test(expected = IOException.class)
  public void test_streamTaskPayload_exceptionWhenCheckingBlobExistence_throwsException() throws Exception
  {
    final String blobPath = PREFIX + "/" + TASK_ID + "/task.json";
    EasyMock.expect(azureStorage.getBlockBlobExists(CONTAINER, blobPath)).andThrow(new BlobStorageException("", null, null));

    replayAll();

    azureTaskLogs.streamTaskPayload(TASK_ID);

    verifyAll();
  }


  @Test
  public void test_killAll_noException_deletesAllTaskLogs() throws Exception
  {
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();

    CloudBlobHolder object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);
    CloudBlobHolder object2 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_2, TIME_1);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(object1, object2),
        azureStorage
    );

    EasyMock.replay(object1, object2);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(object1, object2),
        ImmutableMap.of(),
        MAX_TRIES
    );
    EasyMock.replay(inputDataConfig, accountConfig, timeSupplier, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    azureTaskLogs.killAll();
    EasyMock.verify(inputDataConfig, accountConfig, timeSupplier, object1, object2, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    CloudBlobHolder object1 = null;
    AzureCloudBlobIterable azureCloudBlobIterable = null;
    try {
      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);
      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();

      object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

      azureCloudBlobIterable = AzureTestUtils.expectListObjects(
          azureCloudBlobIterableFactory,
          MAX_KEYS,
          PREFIX_URI,
          ImmutableList.of(object1),
          azureStorage
      );

      EasyMock.replay(object1);
      AzureTestUtils.expectDeleteObjects(
          azureStorage,
          ImmutableList.of(),
          ImmutableMap.of(object1, NON_RECOVERABLE_EXCEPTION),
          MAX_TRIES
      );
      EasyMock.replay(
          inputDataConfig,
          accountConfig,
          timeSupplier,
          azureCloudBlobIterable,
          azureCloudBlobIterableFactory,
          azureStorage
      );
      azureTaskLogs.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }
    Assert.assertTrue(ioExceptionThrown);
    EasyMock.verify(
        inputDataConfig,
        accountConfig,
        timeSupplier,
        object1,
        azureCloudBlobIterable,
        azureCloudBlobIterableFactory,
        azureStorage
    );
  }

  @Test
  public void test_killOlderThan_noException_deletesOnlyTaskLogsOlderThan() throws Exception
  {
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();

    CloudBlobHolder object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);
    CloudBlobHolder object2 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_2, TIME_FUTURE);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(object1, object2),
        azureStorage
    );

    EasyMock.replay(object1, object2);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(object1),
        ImmutableMap.of(),
        MAX_TRIES
    );
    EasyMock.replay(inputDataConfig, accountConfig, timeSupplier, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    azureTaskLogs.killOlderThan(TIME_NOW);
    EasyMock.verify(inputDataConfig, accountConfig, timeSupplier, object1, object2, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killOlderThan_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    CloudBlobHolder object1 = null;
    AzureCloudBlobIterable azureCloudBlobIterable = null;
    try {
      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();

      object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

      azureCloudBlobIterable = AzureTestUtils.expectListObjects(
          azureCloudBlobIterableFactory,
          MAX_KEYS,
          PREFIX_URI,
          ImmutableList.of(object1),
          azureStorage
      );

      EasyMock.replay(object1);
      AzureTestUtils.expectDeleteObjects(
          azureStorage,
          ImmutableList.of(),
          ImmutableMap.of(object1, NON_RECOVERABLE_EXCEPTION),
          MAX_TRIES
      );
      EasyMock.replay(
          inputDataConfig,
          accountConfig,
          timeSupplier,
          azureCloudBlobIterable,
          azureCloudBlobIterableFactory,
          azureStorage
      );
      azureTaskLogs.killOlderThan(TIME_NOW);
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }
    Assert.assertTrue(ioExceptionThrown);
    EasyMock.verify(
        inputDataConfig,
        accountConfig,
        timeSupplier,
        object1,
        azureCloudBlobIterable,
        azureCloudBlobIterableFactory,
        azureStorage
    );
  }

  /*
  @Test (expected = UnsupportedOperationException.class)
  public void test_killOlderThan_throwsUnsupportedOperationException() throws IOException
  {
    azureTaskLogs.killOlderThan(0);
  }
   */

  @After
  public void cleanup()
  {
    resetAll();
  }
}
