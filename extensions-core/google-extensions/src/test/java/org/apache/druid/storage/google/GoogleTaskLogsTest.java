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

package org.apache.druid.storage.google;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.druid.common.utils.CurrentTimeMillisSupplier;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class GoogleTaskLogsTest extends EasyMockSupport
{
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final String BUCKET = "test";
  private static final String PREFIX = "test/log";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("gs://%s/%s", BUCKET, PREFIX));
  private static final String TASKID = "taskid";
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final long TIME_NOW = 2L;
  private static final long TIME_FUTURE = 3L;
  private static final int MAX_KEYS = 1;
  private static final Exception RECOVERABLE_EXCEPTION = new HttpResponseException.Builder(429, "recoverable", new HttpHeaders()).build();
  private static final Exception NON_RECOVERABLE_EXCEPTION = new HttpResponseException.Builder(404, "non recoverable", new HttpHeaders()).build();

  private GoogleStorage storage;
  private GoogleTaskLogs googleTaskLogs;
  private GoogleTaskLogsConfig config;
  private GoogleInputDataConfig inputDataConfig;
  private CurrentTimeMillisSupplier timeSupplier;

  @Before
  public void before()
  {
    storage = createMock(GoogleStorage.class);
    inputDataConfig = createMock(GoogleInputDataConfig.class);
    timeSupplier = createMock(CurrentTimeMillisSupplier.class);

    config = new GoogleTaskLogsConfig(BUCKET, PREFIX);
    googleTaskLogs = new GoogleTaskLogs(config, storage, inputDataConfig, timeSupplier);
  }

  @Test
  public void testPushTaskLog() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File logFile = new File(tmpDir, "log");
      BufferedWriter output = Files.newBufferedWriter(logFile.toPath(), StandardCharsets.UTF_8);
      output.write("test");
      output.close();

      storage.insert(
          EasyMock.eq(BUCKET),
          EasyMock.eq(PREFIX + "/" + TASKID),
          EasyMock.anyObject(InputStreamContent.class)
      );
      EasyMock.expectLastCall();

      replayAll();

      googleTaskLogs.pushTaskLog(TASKID, logFile);

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void testPushTaskStatus() throws Exception
  {
    final File tmpDir = FileUtils.createTempDir();

    try {
      final File statusFile = new File(tmpDir, "status.json");
      BufferedWriter output = Files.newBufferedWriter(statusFile.toPath(), StandardCharsets.UTF_8);
      output.write("{}");
      output.close();

      storage.insert(
          EasyMock.eq(BUCKET),
          EasyMock.eq(PREFIX + "/" + TASKID),
          EasyMock.anyObject(InputStreamContent.class)
      );
      EasyMock.expectLastCall();

      replayAll();

      googleTaskLogs.pushTaskLog(TASKID, statusFile);

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

    final String logPath = PREFIX + "/" + TASKID;
    EasyMock.expect(storage.exists(BUCKET, logPath)).andReturn(true);
    EasyMock.expect(storage.size(BUCKET, logPath)).andReturn((long) testLog.length());
    EasyMock.expect(storage.getInputStream(BUCKET, logPath, 0)).andReturn(new ByteArrayInputStream(StringUtils.toUtf8(testLog)));

    replayAll();

    final Optional<InputStream> stream = googleTaskLogs.streamTaskLog(TASKID, 0);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog);

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithPositiveOffset() throws Exception
  {
    final String testLog = "hello this is a log";
    final int offset = 5;
    final String expectedLog = testLog.substring(offset);
    final String logPath = PREFIX + "/" + TASKID;
    EasyMock.expect(storage.exists(BUCKET, logPath)).andReturn(true);
    EasyMock.expect(storage.size(BUCKET, logPath)).andReturn((long) testLog.length());
    EasyMock.expect(storage.getInputStream(BUCKET, logPath, offset))
            .andReturn(new ByteArrayInputStream(StringUtils.toUtf8(expectedLog)));

    replayAll();

    final Optional<InputStream> stream = googleTaskLogs.streamTaskLog(TASKID, offset);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), expectedLog);

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithNegative() throws Exception
  {
    final String testLog = "hello this is a log";
    final int offset = -3;
    final int internalOffset = testLog.length() + offset;
    final String expectedLog = testLog.substring(internalOffset);
    final String logPath = PREFIX + "/" + TASKID;
    EasyMock.expect(storage.exists(BUCKET, logPath)).andReturn(true);
    EasyMock.expect(storage.size(BUCKET, logPath)).andReturn((long) testLog.length());
    EasyMock.expect(storage.getInputStream(BUCKET, logPath, internalOffset))
            .andReturn(new ByteArrayInputStream(StringUtils.toUtf8(expectedLog)));

    replayAll();

    final Optional<InputStream> stream = googleTaskLogs.streamTaskLog(TASKID, offset);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), expectedLog);

    verifyAll();
  }

  @Test
  public void testStreamTaskStatus() throws Exception
  {
    final String taskStatus = "{}";

    final String logPath = PREFIX + "/" + TASKID + ".status.json";
    EasyMock.expect(storage.exists(BUCKET, logPath)).andReturn(true);
    EasyMock.expect(storage.size(BUCKET, logPath)).andReturn((long) taskStatus.length());
    EasyMock.expect(storage.getInputStream(BUCKET, logPath, 0)).andReturn(new ByteArrayInputStream(StringUtils.toUtf8(taskStatus)));

    replayAll();

    final Optional<InputStream> stream = googleTaskLogs.streamTaskStatus(TASKID);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(stream.get(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), taskStatus);

    verifyAll();
  }

  @Test
  public void test_killAll_noException_deletesAllTaskLogs() throws IOException
  {
    GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);
    GoogleStorageObjectMetadata object2 = GoogleTestUtils.newStorageObject(BUCKET, KEY_2, TIME_1);

    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);
    GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1, object2));

    GoogleTestUtils.expectDeleteObjects(
        storage,
        ImmutableList.of(object1, object2),
        ImmutableMap.of()
    );
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

    EasyMock.replay(inputDataConfig, storage, timeSupplier);

    googleTaskLogs.killAll();

    EasyMock.verify(inputDataConfig, storage, timeSupplier);
  }


  @Test
  public void test_killAll_recoverableExceptionWhenDeletingObjects_deletesAllTaskLogs() throws IOException
  {
    GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);

    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1));

    GoogleTestUtils.expectDeleteObjects(
        storage,
        ImmutableList.of(object1),
        ImmutableMap.of(object1, RECOVERABLE_EXCEPTION)
    );

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

    EasyMock.replay(inputDataConfig, storage, timeSupplier);

    googleTaskLogs.killAll();

    EasyMock.verify(inputDataConfig, storage, timeSupplier);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    try {
      GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);

      EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

      GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1));

      GoogleTestUtils.expectDeleteObjects(
          storage,
          ImmutableList.of(),
          ImmutableMap.of(object1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

      EasyMock.replay(inputDataConfig, storage, timeSupplier);

      googleTaskLogs.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(inputDataConfig, storage, timeSupplier);
  }

  @Test
  public void test_killOlderThan_noException_deletesOnlyTaskLogsOlderThan() throws IOException
  {
    GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);
    GoogleStorageObjectMetadata object2 = GoogleTestUtils.newStorageObject(BUCKET, KEY_2, TIME_FUTURE);

    GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1, object2));

    GoogleTestUtils.expectDeleteObjects(
        storage,
        ImmutableList.of(object1),
        ImmutableMap.of()
    );
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

    EasyMock.replay(inputDataConfig, storage);
    googleTaskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(inputDataConfig, storage);
  }

  @Test
  public void test_killOlderThan_recoverableExceptionWhenListingObjects_deletesAllTaskLogs() throws IOException
  {
    GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);

    GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1));

    GoogleTestUtils.expectDeleteObjects(
        storage,
        ImmutableList.of(object1),
        ImmutableMap.of(object1, RECOVERABLE_EXCEPTION)
    );

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

    EasyMock.replay(inputDataConfig, storage);

    googleTaskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(inputDataConfig, storage);
  }

  @Test
  public void test_killOlderThan_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    try {
      GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);

      GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1));

      GoogleTestUtils.expectDeleteObjects(
          storage,
          ImmutableList.of(),
          ImmutableMap.of(object1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

      EasyMock.replay(inputDataConfig, storage);

      googleTaskLogs.killOlderThan(TIME_NOW);
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(inputDataConfig, storage);
  }
}
