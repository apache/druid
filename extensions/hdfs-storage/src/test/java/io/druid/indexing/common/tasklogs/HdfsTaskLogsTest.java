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

package io.druid.indexing.common.tasklogs;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogs;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import io.druid.tasklogs.TaskLogs;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class HdfsTaskLogsTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testStream() throws Exception
  {
    final File tmpDir = tempFolder.newFolder();
    final File logDir = new File(tmpDir, "logs");
    final File logFile = new File(tmpDir, "log");
    Files.write("blah", logFile, Charsets.UTF_8);
    final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()), new Configuration());
    taskLogs.pushTaskLog("foo", logFile);

    final Map<Long, String> expected = ImmutableMap.of(0L, "blah", 1L, "lah", -2L, "ah", -5L, "blah");
    for (Map.Entry<Long, String> entry : expected.entrySet()) {
      final String string = readLog(taskLogs, entry.getKey());
      Assert.assertEquals(String.format("Read with offset %,d", entry.getKey()), string, entry.getValue());
    }
  }

  @Test
  public void testOverwrite() throws Exception
  {
    final File tmpDir = tempFolder.newFolder();
    final File logDir = new File(tmpDir, "logs");
    final File logFile = new File(tmpDir, "log");
    final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()), new Configuration());

    Files.write("blah", logFile, Charsets.UTF_8);
    taskLogs.pushTaskLog("foo", logFile);
    Assert.assertEquals("blah", readLog(taskLogs, 0));

    Files.write("blah blah", logFile, Charsets.UTF_8);
    taskLogs.pushTaskLog("foo", logFile);
    Assert.assertEquals("blah blah", readLog(taskLogs, 0));
  }

  private String readLog(TaskLogs taskLogs, long offset) throws IOException
  {
    return new String(
        ByteStreams.toByteArray(taskLogs.streamTaskLog("foo", offset).get().openStream()),
        Charsets.UTF_8
    );
  }
}
