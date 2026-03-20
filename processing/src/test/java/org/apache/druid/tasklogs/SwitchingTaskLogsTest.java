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

package org.apache.druid.tasklogs;

import com.google.common.base.Optional;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@RunWith(EasyMockRunner.class)
public class SwitchingTaskLogsTest extends EasyMockSupport
{
  @Mock
  private TaskLogs defaultTaskLogs;

  @Mock
  private TaskLogs reportTaskLogs;

  @Mock
  private TaskLogs streamerTaskLogs;

  @Mock
  private TaskLogs pusherTaskLogs;

  private SwitchingTaskLogs taskLogs;

  @Before
  public void setUp()
  {
    taskLogs = new SwitchingTaskLogs(defaultTaskLogs, reportTaskLogs, streamerTaskLogs, pusherTaskLogs);
  }

  @Test
  public void test_streamTaskLog_usesStreamerTaskLogs() throws IOException
  {
    String taskId = "test-task-id";
    long offset = 0L;
    InputStream logStream = new ByteArrayInputStream("test log content".getBytes(StandardCharsets.UTF_8));

    EasyMock.expect(streamerTaskLogs.streamTaskLog(taskId, offset)).andReturn(Optional.of(logStream));
    replayAll();

    Optional<InputStream> actualLogStream = taskLogs.streamTaskLog(taskId, offset);
    Assert.assertTrue(actualLogStream.isPresent());
    Assert.assertEquals(logStream, actualLogStream.get());

    verifyAll();
  }

  @Test
  public void test_streamTaskReports_usesReportTaskLogs() throws IOException
  {
    String taskId = "test-task-id";
    InputStream reportStream = new ByteArrayInputStream("test report content".getBytes(StandardCharsets.UTF_8));

    EasyMock.expect(reportTaskLogs.streamTaskReports(taskId)).andReturn(Optional.of(reportStream));
    replayAll();

    Optional<InputStream> actualReportStream = taskLogs.streamTaskReports(taskId);
    Assert.assertTrue(actualReportStream.isPresent());
    Assert.assertEquals(reportStream, actualReportStream.get());

    verifyAll();
  }

  @Test
  public void test_streamTaskStatus_usesReportTaskLogs() throws IOException
  {
    String taskId = "test-task-id";
    InputStream statusStream = new ByteArrayInputStream("test status content".getBytes(StandardCharsets.UTF_8));

    EasyMock.expect(reportTaskLogs.streamTaskStatus(taskId)).andReturn(Optional.of(statusStream));
    replayAll();

    Optional<InputStream> actualStatusStream = taskLogs.streamTaskStatus(taskId);
    Assert.assertTrue(actualStatusStream.isPresent());
    Assert.assertEquals(statusStream, actualStatusStream.get());

    verifyAll();
  }

  @Test
  public void test_streamTaskPayload_usesReportTaskLogs() throws IOException
  {
    String taskId = "test-task-id";
    InputStream payloadStream = new ByteArrayInputStream("test payload content".getBytes(StandardCharsets.UTF_8));

    EasyMock.expect(reportTaskLogs.streamTaskPayload(taskId)).andReturn(Optional.of(payloadStream));
    replayAll();

    Optional<InputStream> actualPayloadStream = taskLogs.streamTaskPayload(taskId);
    Assert.assertTrue(actualPayloadStream.isPresent());
    Assert.assertEquals(payloadStream, actualPayloadStream.get());

    verifyAll();
  }

  @Test
  public void test_pushTaskLog_usesPusherTaskLogs() throws IOException
  {
    String taskId = "test-task-id";
    File logFile = new File("test.log");

    pusherTaskLogs.pushTaskLog(taskId, logFile);
    EasyMock.expectLastCall();
    replayAll();

    taskLogs.pushTaskLog(taskId, logFile);

    verifyAll();
  }

  @Test
  public void test_pushTaskReports_usesReportTaskLogs() throws IOException
  {
    String taskId = "test-task-id";
    File logFile = new File("test.log");

    reportTaskLogs.pushTaskReports(taskId, logFile);
    EasyMock.expectLastCall();
    replayAll();

    taskLogs.pushTaskReports(taskId, logFile);

    verifyAll();
  }

  @Test
  public void test_pushTaskStatus_usesReportTaskLogs() throws IOException
  {
    String taskId = "test-task-id";
    File logFile = new File("test.log");

    reportTaskLogs.pushTaskStatus(taskId, logFile);
    EasyMock.expectLastCall();
    replayAll();

    taskLogs.pushTaskStatus(taskId, logFile);

    verifyAll();
  }

  @Test
  public void test_pushTaskPayload_usesReportTaskLogs() throws IOException
  {
    String taskId = "test-task-id";
    File logFile = new File("test.log");

    reportTaskLogs.pushTaskPayload(taskId, logFile);
    EasyMock.expectLastCall();
    replayAll();

    taskLogs.pushTaskPayload(taskId, logFile);

    verifyAll();
  }

  @Test
  public void test_killAll_usesReportTaskLogs() throws IOException
  {
    reportTaskLogs.killAll();
    EasyMock.expectLastCall();
    replayAll();

    taskLogs.killAll();

    verifyAll();
  }

  @Test
  public void test_killOlderThan_usesReportTaskLogs() throws IOException
  {
    long timestamp = System.currentTimeMillis();

    reportTaskLogs.killOlderThan(timestamp);
    EasyMock.expectLastCall();
    replayAll();

    taskLogs.killOlderThan(timestamp);

    verifyAll();
  }
}
