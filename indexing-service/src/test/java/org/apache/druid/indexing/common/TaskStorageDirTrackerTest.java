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

package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TaskStorageDirTrackerTest
{
  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  @Test
  public void testGetOrSelectTaskDir() throws IOException
  {
    File tmpFolder = TMP.newFolder();
    List<File> files = ImmutableList.of(
        new File(tmpFolder, "A"),
        new File(tmpFolder, "B"),
        new File(tmpFolder, "C")
    );

    final TaskStorageDirTracker tracker = new TaskStorageDirTracker(files);
    tracker.ensureDirectories();

    validateRoundRobinAllocation(tmpFolder, tracker);
    for (File file : Objects.requireNonNull(tmpFolder.listFiles())) {
      FileUtils.deleteDirectory(file);
    }

    final TaskStorageDirTracker otherTracker = TaskStorageDirTracker.fromConfigs(
        new WorkerConfig()
        {
          @Override
          public List<String> getBaseTaskDirs()
          {
            return files.stream().map(File::toString).collect(Collectors.toList());
          }
        },
        null
    );
    otherTracker.ensureDirectories();
    validateRoundRobinAllocation(tmpFolder, otherTracker);
  }

  private void validateRoundRobinAllocation(File tmpFolder, TaskStorageDirTracker dirTracker) throws IOException
  {
    // Test round-robin allocation, it starts from "C" and goes "backwards" because the counter is initialized
    // negatively, which modulos to 2 -> 1 -> 0
    Assert.assertEquals(new File(tmpFolder, "C").toString(), dirTracker.pickBaseDir("task0").getPath());
    Assert.assertEquals(new File(tmpFolder, "B").toString(), dirTracker.pickBaseDir("task1").getPath());
    Assert.assertEquals(new File(tmpFolder, "A").toString(), dirTracker.pickBaseDir("task2").getPath());
    Assert.assertEquals(new File(tmpFolder, "C").toString(), dirTracker.pickBaseDir("task3").getPath());
    Assert.assertEquals(new File(tmpFolder, "B").toString(), dirTracker.pickBaseDir("task4").getPath());
    Assert.assertEquals(new File(tmpFolder, "A").toString(), dirTracker.pickBaseDir("task5").getPath());

    // Test that the result is always the same
    FileUtils.mkdirp(new File(new File(tmpFolder, "C"), "task0"));
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(new File(tmpFolder, "C").toString(), dirTracker.pickBaseDir("task0").getPath());
    }
  }

  @Test
  public void testFallBackToTaskConfig() throws IOException
  {
    final File baseDir = new File(TMP.newFolder(), "A");
    final TaskStorageDirTracker tracker = TaskStorageDirTracker.fromConfigs(
        new WorkerConfig(),
        new TaskConfigBuilder().setBaseDir(baseDir.toString()).build()
    );
    tracker.ensureDirectories();

    final String expected = new File(baseDir, "persistent/task").toString();
    Assert.assertEquals(expected, tracker.pickBaseDir("task0").getPath());
    Assert.assertEquals(expected, tracker.pickBaseDir("task1").getPath());
    Assert.assertEquals(expected, tracker.pickBaseDir("task2").getPath());
    Assert.assertEquals(expected, tracker.pickBaseDir("task3").getPath());
    Assert.assertEquals(expected, tracker.pickBaseDir("task1").getPath());
    Assert.assertEquals(expected, tracker.pickBaseDir("task10293721").getPath());
  }
}
