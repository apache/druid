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
import io.druid.indexing.common.config.FileTaskLogsConfig;
import io.druid.tasklogs.TaskLogs;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class FileTaskLogsTest
{

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSimple() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    try {
      final File logDir = new File(tmpDir, "druid/logs");
      final File logFile = new File(tmpDir, "log");
      Files.write("blah", logFile, Charsets.UTF_8);
      final TaskLogs taskLogs = new FileTaskLogs(new FileTaskLogsConfig(logDir));
      taskLogs.pushTaskLog("foo", logFile);

      final Map<Long, String> expected = ImmutableMap.of(0L, "blah", 1L, "lah", -2L, "ah", -5L, "blah");
      for (Map.Entry<Long, String> entry : expected.entrySet()) {
        final byte[] bytes = ByteStreams.toByteArray(taskLogs.streamTaskLog("foo", entry.getKey()).get().getInput());
        final String string = new String(bytes);
        Assert.assertEquals(String.format("Read with offset %,d", entry.getKey()), string, entry.getValue());
      }
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void testPushTaskLogDirCreationFails() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    
    try {
      IOException thrown = null;
      
      final File logDir = new File(tmpDir, "druid/logs");
      final File logFile = new File(tmpDir, "log");
      Files.write("blah", logFile, Charsets.UTF_8);
      
      if(!tmpDir.setWritable(false)) {
        new RuntimeException("failed to make tmp dir read-only");
      }

      final TaskLogs taskLogs = new FileTaskLogs(new FileTaskLogsConfig(logDir));
      try {
        taskLogs.pushTaskLog("foo", logFile);
      } catch(IOException ex) {
        thrown = ex;
      }
      Assert.assertNotNull("pushTaskLog should fail with exception of dir creation error", thrown);
    }
    finally {
      tmpDir.setWritable(true);
      FileUtils.deleteDirectory(tmpDir);
    }
  }
}
