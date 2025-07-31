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

package org.apache.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.tasklogs.TaskLogs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SwitchingTaskLogsTest
{
  private static final String TASK_ID = "testTask";
  private static final String LOG_CONTENT = "test log content";
  private static final String REPORT_CONTENT = "test report content";
  private static final String STATUS_CONTENT = "test status content";
  private static final String PAYLOAD_CONTENT = "test payload content";
  private static final long OFFSET = 100L;
  private static final long TIMESTAMP = 1234567890L;

  @Mock
  private TaskLogs delegateTaskLogs;

  @Mock
  private TaskLogs streamerTaskLogs;

  @Mock
  private TaskLogs pusherTaskLogs;

  @Mock
  private File testFile;

  private SwitchingTaskLogs taskLogs;

  @Before
  public void setUp()
  {
    MockitoAnnotations.initMocks(this);
    taskLogs = new SwitchingTaskLogs(delegateTaskLogs, streamerTaskLogs, pusherTaskLogs);
  }

  @Test
  public void testStreamTaskLog() throws IOException
  {
    InputStream logStream = new ByteArrayInputStream(LOG_CONTENT.getBytes(StandardCharsets.UTF_8));
    when(streamerTaskLogs.streamTaskLog(TASK_ID, OFFSET)).thenReturn(Optional.of(logStream));

    Optional<InputStream> result = taskLogs.streamTaskLog(TASK_ID, OFFSET);

    Assert.assertTrue(result.isPresent());
    String content = StringUtils.fromUtf8(ByteStreams.toByteArray(result.get()));
    Assert.assertEquals(LOG_CONTENT, content);
    verify(streamerTaskLogs).streamTaskLog(TASK_ID, OFFSET);
  }

  @Test
  public void testStreamTaskLogEmpty() throws IOException
  {
    when(streamerTaskLogs.streamTaskLog(TASK_ID, OFFSET)).thenReturn(Optional.<InputStream>absent());

    Optional<InputStream> result = taskLogs.streamTaskLog(TASK_ID, OFFSET);

    Assert.assertFalse(result.isPresent());
    verify(streamerTaskLogs).streamTaskLog(TASK_ID, OFFSET);
  }

  @Test
  public void testStreamTaskLogException() throws IOException
  {
    IOException expectedException = new IOException("Test exception");
    when(streamerTaskLogs.streamTaskLog(TASK_ID, OFFSET)).thenThrow(expectedException);

    try {
      taskLogs.streamTaskLog(TASK_ID, OFFSET);
      Assert.fail("Expected IOException");
    }
    catch (IOException e) {
      Assert.assertEquals("Test exception", e.getMessage());
    }
    verify(streamerTaskLogs).streamTaskLog(TASK_ID, OFFSET);
  }

  @Test
  public void testStreamTaskReports() throws IOException
  {
    InputStream reportStream = new ByteArrayInputStream(REPORT_CONTENT.getBytes(StandardCharsets.UTF_8));
    when(delegateTaskLogs.streamTaskReports(TASK_ID)).thenReturn(Optional.of(reportStream));

    Optional<InputStream> result = taskLogs.streamTaskReports(TASK_ID);

    Assert.assertTrue(result.isPresent());
    String content = StringUtils.fromUtf8(ByteStreams.toByteArray(result.get()));
    Assert.assertEquals(REPORT_CONTENT, content);
    verify(delegateTaskLogs).streamTaskReports(TASK_ID);
  }

  @Test
  public void testStreamTaskReportsEmpty() throws IOException
  {
    when(delegateTaskLogs.streamTaskReports(TASK_ID)).thenReturn(Optional.<InputStream>absent());

    Optional<InputStream> result = taskLogs.streamTaskReports(TASK_ID);

    Assert.assertFalse(result.isPresent());
    verify(delegateTaskLogs).streamTaskReports(TASK_ID);
  }

  @Test
  public void testStreamTaskStatus() throws IOException
  {
    InputStream statusStream = new ByteArrayInputStream(STATUS_CONTENT.getBytes(StandardCharsets.UTF_8));
    when(delegateTaskLogs.streamTaskReports(TASK_ID)).thenReturn(Optional.of(statusStream));

    Optional<InputStream> result = taskLogs.streamTaskStatus(TASK_ID);

    Assert.assertTrue(result.isPresent());
    String content = StringUtils.fromUtf8(ByteStreams.toByteArray(result.get()));
    Assert.assertEquals(STATUS_CONTENT, content);
    verify(delegateTaskLogs).streamTaskReports(TASK_ID);
  }

  @Test
  public void testStreamTaskStatusEmpty() throws IOException
  {
    when(delegateTaskLogs.streamTaskReports(TASK_ID)).thenReturn(Optional.<InputStream>absent());

    Optional<InputStream> result = taskLogs.streamTaskStatus(TASK_ID);

    Assert.assertFalse(result.isPresent());
    verify(delegateTaskLogs).streamTaskReports(TASK_ID);
  }

  @Test
  public void testPushTaskLog() throws IOException
  {
    taskLogs.pushTaskLog(TASK_ID, testFile);

    verify(pusherTaskLogs).pushTaskLog(TASK_ID, testFile);
  }

  @Test
  public void testPushTaskPayload() throws IOException
  {
    taskLogs.pushTaskPayload(TASK_ID, testFile);

    verify(delegateTaskLogs).pushTaskPayload(TASK_ID, testFile);
  }

  @Test
  public void testPushTaskPayloadException() throws IOException
  {
    IOException expectedException = new IOException("Push payload failed");
    doThrow(expectedException).when(delegateTaskLogs).pushTaskPayload(TASK_ID, testFile);

    try {
      taskLogs.pushTaskPayload(TASK_ID, testFile);
      Assert.fail("Expected IOException");
    }
    catch (IOException e) {
      Assert.assertEquals("Push payload failed", e.getMessage());
    }
    verify(delegateTaskLogs).pushTaskPayload(TASK_ID, testFile);
  }

  @Test
  public void testKillAll() throws IOException
  {
    taskLogs.killAll();

    verify(delegateTaskLogs).killAll();
  }

  @Test
  public void testKillAllException() throws IOException
  {
    IOException expectedException = new IOException("Kill all failed");
    doThrow(expectedException).when(delegateTaskLogs).killAll();

    try {
      taskLogs.killAll();
      Assert.fail("Expected IOException");
    }
    catch (IOException e) {
      Assert.assertEquals("Kill all failed", e.getMessage());
    }
    verify(delegateTaskLogs).killAll();
  }

  @Test
  public void testKillOlderThan() throws IOException
  {
    taskLogs.killOlderThan(TIMESTAMP);

    verify(delegateTaskLogs).killOlderThan(TIMESTAMP);
  }

  @Test
  public void testKillOlderThanException() throws IOException
  {
    IOException expectedException = new IOException("Kill older than failed");
    doThrow(expectedException).when(delegateTaskLogs).killOlderThan(TIMESTAMP);

    try {
      taskLogs.killOlderThan(TIMESTAMP);
      Assert.fail("Expected IOException");
    }
    catch (IOException e) {
      Assert.assertEquals("Kill older than failed", e.getMessage());
    }
    verify(delegateTaskLogs).killOlderThan(TIMESTAMP);
  }

  @Test
  public void testPushTaskReports() throws IOException
  {
    taskLogs.pushTaskReports(TASK_ID, testFile);

    verify(delegateTaskLogs).pushTaskReports(TASK_ID, testFile);
  }

  @Test
  public void testPushTaskReportsException() throws IOException
  {
    IOException expectedException = new IOException("Push reports failed");
    doThrow(expectedException).when(delegateTaskLogs).pushTaskReports(TASK_ID, testFile);

    try {
      taskLogs.pushTaskReports(TASK_ID, testFile);
      Assert.fail("Expected IOException");
    }
    catch (IOException e) {
      Assert.assertEquals("Push reports failed", e.getMessage());
    }
    verify(delegateTaskLogs).pushTaskReports(TASK_ID, testFile);
  }

  @Test
  public void testPushTaskStatus() throws IOException
  {
    taskLogs.pushTaskStatus(TASK_ID, testFile);

    verify(delegateTaskLogs).pushTaskStatus(TASK_ID, testFile);
  }

  @Test
  public void testPushTaskStatusException() throws IOException
  {
    IOException expectedException = new IOException("Push status failed");
    doThrow(expectedException).when(delegateTaskLogs).pushTaskStatus(TASK_ID, testFile);

    try {
      taskLogs.pushTaskStatus(TASK_ID, testFile);
      Assert.fail("Expected IOException");
    }
    catch (IOException e) {
      Assert.assertEquals("Push status failed", e.getMessage());
    }
    verify(delegateTaskLogs).pushTaskStatus(TASK_ID, testFile);
  }

  @Test
  public void testStreamTaskPayload() throws IOException
  {
    InputStream payloadStream = new ByteArrayInputStream(PAYLOAD_CONTENT.getBytes(StandardCharsets.UTF_8));
    when(delegateTaskLogs.streamTaskPayload(TASK_ID)).thenReturn(Optional.of(payloadStream));

    Optional<InputStream> result = taskLogs.streamTaskPayload(TASK_ID);

    Assert.assertTrue(result.isPresent());
    String content = StringUtils.fromUtf8(ByteStreams.toByteArray(result.get()));
    Assert.assertEquals(PAYLOAD_CONTENT, content);
    verify(delegateTaskLogs).streamTaskPayload(TASK_ID);
  }

  @Test
  public void testStreamTaskPayloadEmpty() throws IOException
  {
    when(delegateTaskLogs.streamTaskPayload(TASK_ID)).thenReturn(Optional.<InputStream>absent());

    Optional<InputStream> result = taskLogs.streamTaskPayload(TASK_ID);

    Assert.assertFalse(result.isPresent());
    verify(delegateTaskLogs).streamTaskPayload(TASK_ID);
  }

  @Test
  public void testStreamTaskPayloadException() throws IOException
  {
    IOException expectedException = new IOException("Stream payload failed");
    when(delegateTaskLogs.streamTaskPayload(TASK_ID)).thenThrow(expectedException);

    try {
      taskLogs.streamTaskPayload(TASK_ID);
      Assert.fail("Expected IOException");
    }
    catch (IOException e) {
      Assert.assertEquals("Stream payload failed", e.getMessage());
    }
    verify(delegateTaskLogs).streamTaskPayload(TASK_ID);
  }
}
