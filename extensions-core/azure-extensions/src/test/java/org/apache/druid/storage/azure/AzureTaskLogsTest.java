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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.microsoft.azure.storage.StorageException;
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
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class AzureTaskLogsTest extends EasyMockSupport
{

  private static final String CONTAINER = "test";
  private static final String PREFIX = "test/log";
  private static final String TASK_ID = "taskid";
  private static final String TASK_ID_NOT_FOUND = "taskidNotFound";
  private static final int MAX_TRIES = 3;
  private static final AzureTaskLogsConfig AZURE_TASK_LOGS_CONFIG = new AzureTaskLogsConfig(CONTAINER, PREFIX, MAX_TRIES);
  private static final int MAX_KEYS = 1;
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final long TIME_NOW = 2L;
  private static final long TIME_FUTURE = 3L;
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("azure://%s/%s", CONTAINER, PREFIX));
  private static final Exception RECOVERABLE_EXCEPTION = new StorageException("", "", null);
  private static final Exception NON_RECOVERABLE_EXCEPTION = new URISyntaxException("", "");

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

  @Test
  public void test_killAll_noException_deletesAllTaskLogs() throws Exception
  {
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();
    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    CloudBlobHolder object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);
    CloudBlobHolder object2 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_2, TIME_1);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(object1, object2));

    EasyMock.replay(object1, object2);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(object1, object2),
        ImmutableMap.of());
    EasyMock.replay(inputDataConfig, accountConfig, timeSupplier, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    azureTaskLogs.killAll();
    EasyMock.verify(inputDataConfig, accountConfig, timeSupplier, object1, object2, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killAll_recoverableExceptionWhenDeletingObjects_deletesAllTaskLogs() throws Exception
  {
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();
    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    CloudBlobHolder object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(object1));

    EasyMock.replay(object1);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(object1),
        ImmutableMap.of(object1, RECOVERABLE_EXCEPTION));
    EasyMock.replay(inputDataConfig, accountConfig, timeSupplier, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    azureTaskLogs.killAll();
    EasyMock.verify(inputDataConfig, accountConfig, timeSupplier, object1, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs() throws Exception
  {
    boolean ioExceptionThrown = false;
    CloudBlobHolder object1 = null;
    AzureCloudBlobIterable azureCloudBlobIterable = null;
    try {
      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();
      EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

      object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

      azureCloudBlobIterable = AzureTestUtils.expectListObjects(
          azureCloudBlobIterableFactory,
          MAX_KEYS,
          PREFIX_URI,
          ImmutableList.of(object1)
      );

      EasyMock.replay(object1);
      AzureTestUtils.expectDeleteObjects(
          azureStorage,
          ImmutableList.of(),
          ImmutableMap.of(object1, NON_RECOVERABLE_EXCEPTION)
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
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();

    CloudBlobHolder object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);
    CloudBlobHolder object2 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_2, TIME_FUTURE);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(object1, object2));

    EasyMock.replay(object1, object2);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(object1),
        ImmutableMap.of());
    EasyMock.replay(inputDataConfig, accountConfig, timeSupplier, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    azureTaskLogs.killOlderThan(TIME_NOW);
    EasyMock.verify(inputDataConfig, accountConfig, timeSupplier, object1, object2, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killOlderThan_recoverableExceptionWhenDeletingObjects_deletesOnlyTaskLogsOlderThan() throws Exception
  {
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();

    CloudBlobHolder object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(object1));

    EasyMock.replay(object1);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(object1),
        ImmutableMap.of(object1, RECOVERABLE_EXCEPTION));
    EasyMock.replay(inputDataConfig, accountConfig, timeSupplier, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    azureTaskLogs.killOlderThan(TIME_NOW);
    EasyMock.verify(inputDataConfig, accountConfig, timeSupplier, object1, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killOlderThan_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs() throws Exception
  {
    boolean ioExceptionThrown = false;
    CloudBlobHolder object1 = null;
    AzureCloudBlobIterable azureCloudBlobIterable = null;
    try {
      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();

      object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

      azureCloudBlobIterable = AzureTestUtils.expectListObjects(
          azureCloudBlobIterableFactory,
          MAX_KEYS,
          PREFIX_URI,
          ImmutableList.of(object1)
      );

      EasyMock.replay(object1);
      AzureTestUtils.expectDeleteObjects(
          azureStorage,
          ImmutableList.of(),
          ImmutableMap.of(object1, NON_RECOVERABLE_EXCEPTION)
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
