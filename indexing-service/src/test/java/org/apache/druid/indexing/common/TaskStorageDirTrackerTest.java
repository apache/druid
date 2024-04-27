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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    final int workerCapacity = 7;
    final int baseTaskDirSize = 100_000_000;

    final TaskStorageDirTracker tracker = TaskStorageDirTracker.fromBaseDirs(files, workerCapacity, baseTaskDirSize);
    tracker.ensureDirectories();

    validateRoundRobinAllocation(tmpFolder, tracker);
    StorageSlotVerifier verifier = new StorageSlotVerifier(tmpFolder).setExpectedSize(33_333_333L);
    verifier.validateException(tracker, "eighth-task");

    tracker.returnStorageSlot(tracker.pickStorageSlot("task3"));
    verifier.validate(tracker.pickStorageSlot("eighth-task"), "A", "slot2");

    final TaskStorageDirTracker otherTracker = TaskStorageDirTracker.fromConfigs(
        new WorkerConfig().cloneBuilder()
                          .setCapacity(workerCapacity)
                          .setBaseTaskDirSize(baseTaskDirSize)
                          .setBaseTaskDirs(files.stream().map(File::toString).collect(Collectors.toList()))
                          .build(),
        null
    );
    otherTracker.ensureDirectories();
    validateRoundRobinAllocation(tmpFolder, otherTracker);
    verifier = new StorageSlotVerifier(tmpFolder).setExpectedSize(33_333_333L);
    verifier.validateException(otherTracker, "eighth-task");

    otherTracker.returnStorageSlot(otherTracker.pickStorageSlot("task3"));
    verifier.validate(otherTracker.pickStorageSlot("eighth-task"), "A", "slot2");

    final IAE iae = Assert.assertThrows(
        IAE.class,
        () -> tracker.returnStorageSlot(otherTracker.pickStorageSlot("eighth-task"))
    );
    Assert.assertEquals(
        "Cannot return storage slot for task [eighth-task] that I don't own.",
        iae.getMessage()
    );
  }

  private void validateRoundRobinAllocation(File tmpFolder, TaskStorageDirTracker dirTracker)
  {
    // Test round-robin allocation, it starts from "C" and goes "backwards" because the counter is initialized
    // negatively, which modulos to 2 -> 1 -> 0
    StorageSlotVerifier verifier = new StorageSlotVerifier(tmpFolder).setExpectedSize(33_333_333L);
    verifier.validate(dirTracker.pickStorageSlot("task0"), "C", "slot0");
    verifier.validate(dirTracker.pickStorageSlot("task1"), "B", "slot0");
    verifier.validate(dirTracker.pickStorageSlot("task2"), "A", "slot0");
    verifier.validate(dirTracker.pickStorageSlot("task3"), "A", "slot2");
    verifier.validate(dirTracker.pickStorageSlot("task4"), "C", "slot1");
    verifier.validate(dirTracker.pickStorageSlot("task5"), "B", "slot1");
    verifier.validate(dirTracker.pickStorageSlot("task6"), "A", "slot1");

    for (int i = 0; i < 10; i++) {
      verifier.validate(dirTracker.pickStorageSlot("task0"), "C", "slot0");
      verifier.validate(dirTracker.pickStorageSlot("task1"), "B", "slot0");
      verifier.validate(dirTracker.pickStorageSlot("task2"), "A", "slot0");
      verifier.validate(dirTracker.pickStorageSlot("task3"), "A", "slot2");
      verifier.validate(dirTracker.pickStorageSlot("task4"), "C", "slot1");
      verifier.validate(dirTracker.pickStorageSlot("task5"), "B", "slot1");
      verifier.validate(dirTracker.pickStorageSlot("task6"), "A", "slot1");
    }
  }

  @Test
  public void testFallBackToTaskConfig() throws IOException
  {
    final File baseDir = new File(TMP.newFolder(), "A");
    final TaskStorageDirTracker tracker = TaskStorageDirTracker.fromConfigs(
        new WorkerConfig()
        {
          @Override
          public int getCapacity()
          {
            return 6;
          }

          @Override
          public long getBaseTaskDirSize()
          {
            return 60_000_000;
          }
        },

        new TaskConfigBuilder().setBaseDir(baseDir.toString()).build()
    );
    tracker.ensureDirectories();

    StorageSlotVerifier verifier = new StorageSlotVerifier(new File(baseDir, "persistent/task"))
        .setExpectedSize(10_000_000L);
    verifier.validate(tracker.pickStorageSlot("task0"), "slot2");
    verifier.validate(tracker.pickStorageSlot("task1"), "slot1");
    verifier.validate(tracker.pickStorageSlot("task2"), "slot0");
    verifier.validate(tracker.pickStorageSlot("task3"), "slot5");
    verifier.validate(tracker.pickStorageSlot("task4"), "slot4");
    verifier.validate(tracker.pickStorageSlot("task10293721"), "slot3");

    Assert.assertThrows(
        ISE.class,
        () -> tracker.pickStorageSlot("seventh-task")
    );
  }

  @Test
  public void testMoreDirectoriesThanSlots() throws IOException
  {
    File tmpFolder = TMP.newFolder();
    List<File> files = ImmutableList.of(
        new File(tmpFolder, "A"),
        new File(tmpFolder, "B"),
        new File(tmpFolder, "C")
    );
    final int workerCapacity = 2;
    final int baseTaskDirSize = 100_000_000;

    final TaskStorageDirTracker tracker = TaskStorageDirTracker.fromConfigs(
        new WorkerConfig().cloneBuilder()
                          .setCapacity(workerCapacity)
                          .setBaseTaskDirSize(baseTaskDirSize)
                          .setBaseTaskDirs(files.stream().map(File::toString).collect(Collectors.toList()))
                          .build(),
        null
    );
    tracker.ensureDirectories();
    StorageSlotVerifier verifier = new StorageSlotVerifier(tmpFolder).setExpectedSize(100_000_000L);
    verifier.validate(tracker.pickStorageSlot("task1"), "A", "slot0");
    verifier.validate(tracker.pickStorageSlot("task2"), "B", "slot0");
    Assert.assertThrows(
        ISE.class,
        () -> tracker.pickStorageSlot("third-task")
    );
  }

  @Test
  public void testMigration() throws IOException
  {
    File tmpFolder = TMP.newFolder();
    List<File> files = ImmutableList.of(new File(tmpFolder, "A"), new File(tmpFolder, "B"));

    TaskStorageDirTracker tracker = TaskStorageDirTracker.fromBaseDirs(files, 4, 100_000_000L);
    tracker.ensureDirectories();

    // First, set a baseline of what would happen without anything pre-defined.
    StorageSlotVerifier verifier = new StorageSlotVerifier(tmpFolder).setExpectedSize(50_000_000L);
    verifier.validate(tracker.pickStorageSlot("task1"), "A", "slot0");
    verifier.validate(tracker.pickStorageSlot("task2"), "B", "slot1");
    verifier.validate(tracker.pickStorageSlot("task3"), "A", "slot1");
    verifier.validate(tracker.pickStorageSlot("task4"), "B", "slot0");
    verifier.validateException(tracker, "fifth-task");


    // Now, clean things up and start with some tasks for migration
    FileUtils.deleteDirectory(tmpFolder);
    FileUtils.mkdirp(new File(files.get(0), "task1"));
    FileUtils.mkdirp(new File(files.get(1), "task3"));
    FileUtils.mkdirp(new File(new File(files.get(0), "slot0"), "task2"));


    tracker = TaskStorageDirTracker.fromBaseDirs(files, 4, 100_000_000L);
    tracker.ensureDirectories();

    // Remove "A/slot1" so that task3 cannot be moved
    FileUtils.deleteDirectory(new File(files.get(0), "slot1"));

    final Map<String, TaskStorageDirTracker.StorageSlot> dirs = tracker.findExistingTaskDirs(
        Arrays.asList("task1", "task2", "task3", "task4")
    );

    Assert.assertNull(dirs.get("task3"));
    Assert.assertNull(dirs.get("task4"));

    // Re-create the dirs so that we can actually pick stuff again.
    tracker.ensureDirectories();

    verifier.validate(dirs.get("task1"), "B", "slot1");
    verifier.validate(dirs.get("task2"), "A", "slot0");
    verifier.validate(tracker.pickStorageSlot("task4"), "B", "slot0");
    verifier.validate(tracker.pickStorageSlot("task5"), "A", "slot1");
    verifier.validateException(tracker, "fifth-task");

    verifier.validate(tracker.pickStorageSlot("task1"), "B", "slot1");
    verifier.validate(tracker.pickStorageSlot("task2"), "A", "slot0");
    verifier.validate(tracker.pickStorageSlot("task4"), "B", "slot0");
    verifier.validate(tracker.pickStorageSlot("task5"), "A", "slot1");
  }

  public static class StorageSlotVerifier
  {
    private final File baseDir;
    private Long expectedSize;

    public StorageSlotVerifier(File baseDir)
    {
      this.baseDir = baseDir;
    }

    public StorageSlotVerifier setExpectedSize(Long expectedSize)
    {
      this.expectedSize = expectedSize;
      return this;
    }

    public TaskStorageDirTracker.StorageSlot validate(TaskStorageDirTracker.StorageSlot slot, String... dirs)
    {
      File theFile = baseDir;
      for (String dir : dirs) {
        theFile = new File(theFile, dir);
      }
      Assert.assertEquals(theFile, slot.getDirectory());
      if (expectedSize != null) {
        Assert.assertEquals(expectedSize.longValue(), slot.getNumBytes());
      }
      return slot;
    }

    public void validateException(TaskStorageDirTracker tracker, String taskId)
    {
      Assert.assertThrows(
          ISE.class,
          () -> tracker.pickStorageSlot(taskId)
      );
    }
  }
}
