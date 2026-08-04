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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.hdfs.tasklog.HdfsTaskLogs;
import org.apache.druid.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import org.apache.druid.tasklogs.TaskLogs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HdfsTaskLogsTest
{
  @TempDir
  public File tempFolder;

  @Test
  public void testStream() throws Exception
  {
    final File tmpDir = newFolder(tempFolder, "junit");
    final File logDir = new File(tmpDir, "logs");
    final File logFile = new File(tmpDir, "log");
    Files.asCharSink(logFile, StandardCharsets.UTF_8).write("blah");
    final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()), new Configuration());
    taskLogs.pushTaskLog("foo", logFile);

    final Map<Long, String> expected = ImmutableMap.of(0L, "blah", 1L, "lah", -2L, "ah", -5L, "blah");
    for (Map.Entry<Long, String> entry : expected.entrySet()) {
      final String string = readLog(taskLogs, "foo", entry.getKey());
      Assertions.assertEquals(string, entry.getValue(), StringUtils.format("Read with offset %,d", entry.getKey()));
    }
  }

  @Test
  public void testOverwrite() throws Exception
  {
    final File tmpDir = newFolder(tempFolder, "junit");
    final File logDir = new File(tmpDir, "logs");
    final File logFile = new File(tmpDir, "log");
    final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()), new Configuration());

    Files.asCharSink(logFile, StandardCharsets.UTF_8).write("blah");
    taskLogs.pushTaskLog("foo", logFile);
    Assertions.assertEquals("blah", readLog(taskLogs, "foo", 0));

    Files.asCharSink(logFile, StandardCharsets.UTF_8).write("blah blah");
    taskLogs.pushTaskLog("foo", logFile);
    Assertions.assertEquals("blah blah", readLog(taskLogs, "foo", 0));
  }

  @Test
  public void test_taskStatus() throws Exception
  {
    final File tmpDir = newFolder(tempFolder, "junit");
    final File logDir = new File(tmpDir, "logs");
    final File statusFile = new File(tmpDir, "status.json");
    final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()), new Configuration());


    Files.asCharSink(statusFile, StandardCharsets.UTF_8).write("{}");
    taskLogs.pushTaskStatus("id", statusFile);
    Assertions.assertEquals(
        "{}",
        StringUtils.fromUtf8(ByteStreams.toByteArray(taskLogs.streamTaskStatus("id").get()))
    );
  }

  @Test
  public void test_taskPayload() throws Exception
  {
    final File tmpDir = newFolder(tempFolder, "junit");
    final File logDir = new File(tmpDir, "logs");
    final File payload = new File(tmpDir, "payload.json");
    final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()), new Configuration());

    Files.asCharSink(payload, StandardCharsets.UTF_8).write("{}");
    taskLogs.pushTaskPayload("id", payload);
    Assertions.assertEquals("{}", StringUtils.fromUtf8(ByteStreams.toByteArray(taskLogs.streamTaskPayload("id").get())));
  }

  @Test
  public void testKill() throws Exception
  {
    final File tmpDir = newFolder(tempFolder, "junit");
    final File logDir = new File(tmpDir, "logs");
    final File logFile = new File(tmpDir, "log");

    final Path logDirPath = new Path(logDir.toString());
    FileSystem fs = new Path(logDir.toString()).getFileSystem(new Configuration());

    final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()), new Configuration());

    Files.asCharSink(logFile, StandardCharsets.UTF_8).write("log1content");
    taskLogs.pushTaskLog("log1", logFile);
    Assertions.assertEquals("log1content", readLog(taskLogs, "log1", 0));

    //File modification timestamp is only maintained to seconds resolution, so artificial delay
    //is necessary to separate 2 file creations by a timestamp that would result in only one
    //of them getting deleted
    Thread.sleep(1500);
    long time = (System.currentTimeMillis() / 1000) * 1000;
    Assertions.assertTrue(fs.getFileStatus(new Path(logDirPath, "log1")).getModificationTime() < time);

    Files.asCharSink(logFile, StandardCharsets.UTF_8).write("log2content");
    taskLogs.pushTaskLog("log2", logFile);
    Assertions.assertEquals("log2content", readLog(taskLogs, "log2", 0));
    Assertions.assertTrue(fs.getFileStatus(new Path(logDirPath, "log2")).getModificationTime() >= time);

    taskLogs.killOlderThan(time);

    Assertions.assertFalse(taskLogs.streamTaskLog("log1", 0).isPresent());
    Assertions.assertEquals("log2content", readLog(taskLogs, "log2", 0));

  }

  private String readLog(TaskLogs taskLogs, String logFile, long offset) throws IOException
  {
    return StringUtils.fromUtf8(ByteStreams.toByteArray(taskLogs.streamTaskLog(logFile, offset).get()));
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
