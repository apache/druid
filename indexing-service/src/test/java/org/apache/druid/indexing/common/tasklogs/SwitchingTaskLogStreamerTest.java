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
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.tasklogs.NoopTaskLogs;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class SwitchingTaskLogStreamerTest
{
  private static final String LOG = "LOG";
  private static final String REPORT = "REPORT";
  private static final String TASK_ID = "foo";

  private final TaskLogStreamer streamer1 = new TestTaskLogStreamer(1);
  private final TaskLogStreamer streamer2 = new TestTaskLogStreamer(2);
  private final TaskLogStreamer emptyStreamer = new NoopTaskLogs();
  private final TaskLogStreamer ioExceptionStreamer = new TaskLogStreamer()
  {
    @Override
    public Optional<InputStream> streamTaskLog(String taskid, long offset) throws IOException
    {
      throw new IOE("expected log exception");
    }

    @Override
    public Optional<InputStream> streamTaskReports(String taskid) throws IOException
    {
      throw new IOE("expected task exception");
    }
  };

  @Test
  public void foundInRemoteTasks() throws IOException
  {
    SwitchingTaskLogStreamer switchingTaskLogStreamer = new SwitchingTaskLogStreamer(
        streamer1,
        Arrays.asList(
            streamer2,
            emptyStreamer
        )
    );
    Assert.assertEquals(
        getLogString(1, TASK_ID, 1),
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskLog(TASK_ID, 1).get()))
    );

    Assert.assertEquals(
        getReportString(1, TASK_ID),
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskReports(TASK_ID).get()))
    );
  }

  @Test
  public void foundInDeepStorage() throws IOException
  {

    SwitchingTaskLogStreamer switchingTaskLogStreamer = new SwitchingTaskLogStreamer(
        emptyStreamer,
        Arrays.asList(
            streamer2,
            emptyStreamer
        )
    );
    Assert.assertEquals(
        getLogString(2, TASK_ID, 1),
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskLog(TASK_ID, 1).get()))
    );

    Assert.assertEquals(
        getReportString(2, TASK_ID),
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskReports(TASK_ID).get()))
    );
  }

  @Test
  public void exceptionInTaskStreamerButFoundInDeepStrorage() throws IOException
  {
    SwitchingTaskLogStreamer switchingTaskLogStreamer = new SwitchingTaskLogStreamer(
        ioExceptionStreamer,
        Arrays.asList(
            streamer2,
            emptyStreamer
        )
    );
    Assert.assertEquals(
        getLogString(2, TASK_ID, 1),
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskLog(TASK_ID, 1).get()))
    );

    Assert.assertEquals(
        getReportString(2, TASK_ID),
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskReports(TASK_ID).get()))
    );
  }


  @Test
  public void exceptionInDeepStrorage()
  {
    SwitchingTaskLogStreamer switchingTaskLogStreamer = new SwitchingTaskLogStreamer(
        emptyStreamer,
        Arrays.asList(
            ioExceptionStreamer,
            streamer2
        )
    );
    Assert.assertThrows("expected log exception", IOException.class, () ->
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskLog(TASK_ID, 1).get()))
    );

    Assert.assertThrows("expected report exception", IOException.class, () ->
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskReports(TASK_ID).get()))
    );
  }

  @Test
  public void exceptionInRemoteTaskLogStreamerWithEmptyDeepStorage()
  {
    SwitchingTaskLogStreamer switchingTaskLogStreamer = new SwitchingTaskLogStreamer(
        ioExceptionStreamer,
        Collections.singletonList(
            emptyStreamer
        )
    );
    Assert.assertThrows("expected log exception", IOException.class, () ->
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskLog(TASK_ID, 1).get()))
    );

    Assert.assertThrows("expected report exception", IOException.class, () ->
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskReports(TASK_ID).get()))
    );

  }

  @Test
  public void exceptionEverywhere()
  {
    SwitchingTaskLogStreamer switchingTaskLogStreamer = new SwitchingTaskLogStreamer(
        ioExceptionStreamer,
        Collections.singletonList(
            ioExceptionStreamer
        )
    );
    Assert.assertThrows("expected log exception", IOException.class, () ->
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskLog(TASK_ID, 1).get()))
    );

    Assert.assertThrows("expected report exception", IOException.class, () ->
        StringUtils.fromUtf8(ByteStreams.toByteArray(switchingTaskLogStreamer.streamTaskReports(TASK_ID).get()))
    );
  }

  @Test
  public void empty() throws IOException
  {
    SwitchingTaskLogStreamer switchingTaskLogStreamer = new SwitchingTaskLogStreamer(
        emptyStreamer,
        Collections.singletonList(
            emptyStreamer
        )
    );
    Assert.assertFalse(switchingTaskLogStreamer.streamTaskLog(TASK_ID, 1).isPresent());
    Assert.assertFalse(switchingTaskLogStreamer.streamTaskReports(TASK_ID).isPresent());
  }

  private static String getLogString(int id, String taskid, long offset)
  {
    return StringUtils.format(
        LOG + " with id %d, task %s and offset %d",
        id,
        taskid,
        offset
    );
  }


  private static String getReportString(int id, String taskid)
  {
    return StringUtils.format(
        REPORT + " with id %d, task %s",
        id,
        taskid
    );
  }

  private static class TestTaskLogStreamer implements TaskLogStreamer
  {
    private final int id;

    public TestTaskLogStreamer(int id)
    {
      this.id = id;
    }

    @Override
    public Optional<InputStream> streamTaskLog(String taskid, long offset)
    {
      return Optional.of(new ByteArrayInputStream(getLogString(id, taskid, offset).getBytes(StandardCharsets.UTF_8)));
    }


    @Override
    public Optional<InputStream> streamTaskReports(String taskid)
    {
      return Optional.of(new ByteArrayInputStream(getReportString(id, taskid).getBytes(StandardCharsets.UTF_8)));
    }
  }
}
