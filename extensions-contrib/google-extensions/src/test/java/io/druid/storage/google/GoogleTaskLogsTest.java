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

package io.druid.storage.google;

import com.google.api.client.http.InputStreamContent;
import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import io.druid.java.util.common.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

public class GoogleTaskLogsTest extends EasyMockSupport
{
  private static final String bucket = "test";
  private static final String prefix = "test/log";
  private static final String taskid = "taskid";

  private GoogleStorage storage;
  private GoogleTaskLogs googleTaskLogs;

  @Before
  public void before()
  {
    storage = createMock(GoogleStorage.class);
    GoogleTaskLogsConfig config = new GoogleTaskLogsConfig(bucket, prefix);
    googleTaskLogs = new GoogleTaskLogs(config, storage);
  }

  @Test
  public void testPushTaskLog() throws Exception
  {
    final File tmpDir = Files.createTempDir();

    try {
      final File logFile = new File(tmpDir, "log");
      BufferedWriter output = java.nio.file.Files.newBufferedWriter(logFile.toPath(), StandardCharsets.UTF_8);
      output.write("test");
      output.close();

      storage.insert(EasyMock.eq(bucket), EasyMock.eq(prefix + "/" + taskid), EasyMock.anyObject(InputStreamContent.class));
      expectLastCall();

      replayAll();

      googleTaskLogs.pushTaskLog(taskid, logFile);

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

    final String logPath = prefix + "/" + taskid;
    expect(storage.exists(bucket, logPath)).andReturn(true);
    expect(storage.size(bucket, logPath)).andReturn((long) testLog.length());
    expect(storage.get(bucket, logPath)).andReturn(new ByteArrayInputStream(StringUtils.toUtf8(testLog)));

    replayAll();

    final Optional<ByteSource> byteSource = googleTaskLogs.streamTaskLog(taskid, 0);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog);

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithPositiveOffset() throws Exception
  {
    final String testLog = "hello this is a log";

    final String logPath = prefix + "/" + taskid;
    expect(storage.exists(bucket, logPath)).andReturn(true);
    expect(storage.size(bucket, logPath)).andReturn((long) testLog.length());
    expect(storage.get(bucket, logPath)).andReturn(new ByteArrayInputStream(StringUtils.toUtf8(testLog)));

    replayAll();

    final Optional<ByteSource> byteSource = googleTaskLogs.streamTaskLog(taskid, 5);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog.substring(5));

    verifyAll();
  }

  @Test
  public void testStreamTaskLogWithNegative() throws Exception
  {
    final String testLog = "hello this is a log";

    final String logPath = prefix + "/" + taskid;
    expect(storage.exists(bucket, logPath)).andReturn(true);
    expect(storage.size(bucket, logPath)).andReturn((long) testLog.length());
    expect(storage.get(bucket, logPath)).andReturn(new ByteArrayInputStream(StringUtils.toUtf8(testLog)));

    replayAll();

    final Optional<ByteSource> byteSource = googleTaskLogs.streamTaskLog(taskid, -3);

    final StringWriter writer = new StringWriter();
    IOUtils.copy(byteSource.get().openStream(), writer, "UTF-8");
    Assert.assertEquals(writer.toString(), testLog.substring(testLog.length() - 3));

    verifyAll();
  }
}
