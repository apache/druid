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
import io.druid.java.util.common.StringUtils;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogs;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import io.druid.tasklogs.TaskLogs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
      final String string = readLog(taskLogs, "foo", entry.getKey());
      Assert.assertEquals(StringUtils.format("Read with offset %,d", entry.getKey()), string, entry.getValue());
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
    Assert.assertEquals("blah", readLog(taskLogs, "foo", 0));

    Files.write("blah blah", logFile, Charsets.UTF_8);
    taskLogs.pushTaskLog("foo", logFile);
    Assert.assertEquals("blah blah", readLog(taskLogs, "foo", 0));
  }

  @Test
  public void testKill() throws Exception
  {
    final File tmpDir = tempFolder.newFolder();
    final File logDir = new File(tmpDir, "logs");
    final File logFile = new File(tmpDir, "log");

    final Path logDirPath = new Path(logDir.toString());
    FileSystem fs = new Path(logDir.toString()).getFileSystem(new Configuration());

    final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()), new Configuration());

    Files.write("log1content", logFile, Charsets.UTF_8);
    taskLogs.pushTaskLog("log1", logFile);
    Assert.assertEquals("log1content", readLog(taskLogs, "log1", 0));

    //File modification timestamp is only maintained to seconds resolution, so artificial delay
    //is necessary to separate 2 file creations by a timestamp that would result in only one
    //of them getting deleted
    Thread.sleep(1500);
    long time = (System.currentTimeMillis()/1000)*1000;
    Assert.assertTrue(fs.getFileStatus(new Path(logDirPath, "log1")).getModificationTime() < time);

    Files.write("log2content", logFile, Charsets.UTF_8);
    taskLogs.pushTaskLog("log2", logFile);
    Assert.assertEquals("log2content", readLog(taskLogs, "log2", 0));
    Assert.assertTrue(fs.getFileStatus(new Path(logDirPath, "log2")).getModificationTime() >= time);

    taskLogs.killOlderThan(time);

    Assert.assertFalse(taskLogs.streamTaskLog("log1", 0).isPresent());
    Assert.assertEquals("log2content", readLog(taskLogs, "log2", 0));

  }

  private String readLog(TaskLogs taskLogs, String logFile, long offset) throws IOException
  {
    return StringUtils.fromUtf8(ByteStreams.toByteArray(taskLogs.streamTaskLog(logFile, offset).get().openStream()));
  }
}
