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
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Used to pick storage slots for tasks when run from the middle manager.
 */
public class TaskStorageDirTracker
{
  public static TaskStorageDirTracker fromConfigs(WorkerConfig workerConfig, TaskConfig taskConfig)
  {
    final List<File> baseTaskDirs;
    if (workerConfig == null) {
      baseTaskDirs = ImmutableList.of(taskConfig.getBaseTaskDir());
    } else {
      final List<String> basePaths = workerConfig.getBaseTaskDirs();
      if (basePaths == null) {
        baseTaskDirs = ImmutableList.of(taskConfig.getBaseTaskDir());
      } else {
        baseTaskDirs = basePaths.stream().map(File::new).collect(Collectors.toList());
      }
    }

    return fromBaseDirs(baseTaskDirs, workerConfig.getCapacity(), workerConfig.getBaseTaskDirSize());
  }

  public static TaskStorageDirTracker fromBaseDirs(List<File> baseTaskDirs, int numSlots, long dirSize)
  {
    int slotsPerBaseTaskDir = Math.max(1, numSlots / baseTaskDirs.size());
    if (numSlots % baseTaskDirs.size() > 0) {
      // We have to add an extra slot per location if they do not evenly divide
      ++slotsPerBaseTaskDir;
    }
    long sizePerSlot = dirSize / slotsPerBaseTaskDir;

    StorageSlot[] slots = new StorageSlot[numSlots];
    for (int i = 0; i < numSlots; ++i) {
      final int whichDir = i % baseTaskDirs.size();
      final int dirUsageCount = i / baseTaskDirs.size();
      final File slotDirectory = new File(baseTaskDirs.get(whichDir), StringUtils.format("slot%d", dirUsageCount));
      slots[i] = new StorageSlot(slotDirectory, sizePerSlot);
    }

    return new TaskStorageDirTracker(baseTaskDirs, slots);
  }

  /**
   * The base task dirs, this field exists primarily for compatibility with scheduling that was done
   * before TaskStorageDirTracker was introduced.  All of the tasks were just splatted together
   * into one directory.  If we want to be able to restore the tasks, we need to be able to find them
   * at the old locations and that is why this exists.
   */
  private final File[] baseTaskDirs;

  /**
   * These are slots pre-divided to keep disk sizing considerations aligned.  The normal operation of this
   * class is to round-robin across these slots.
   */
  private final StorageSlot[] slots;

  /**
   * A counter used to simplify round-robin allocation.  We initialize it to a negative value because it
   * simplifies testing/ensuring that we can handle overflow-rollover of the integer
   */
  private final AtomicInteger iterationCounter = new AtomicInteger(Integer.MIN_VALUE);

  public TaskStorageDirTracker(List<File> baseTaskDirs, StorageSlot[] slots)
  {
    this.baseTaskDirs = baseTaskDirs.toArray(new File[0]);
    this.slots = slots;
  }

  @LifecycleStart
  public void ensureDirectories()
  {
    for (StorageSlot slot : slots) {
      if (!slot.getDirectory().exists()) {
        try {
          FileUtils.mkdirp(slot.getDirectory());
        }
        catch (IOException e) {
          throw new ISE(
              e,
              "directory for slot [%s] likely does not exist, please ensure it exists and the user has permissions.",
              slot
          );
        }
      }
    }
  }

  public synchronized StorageSlot pickStorageSlot(String taskId)
  {
    // if the task directory already exists, we want to give it precedence, so check.
    for (StorageSlot slot : slots) {
      if (slot.runningTaskId != null && slot.runningTaskId.equals(taskId)) {
        return slot;
      }
    }

    // if it doesn't exist, pick one round-robin and ensure it is unused.
    for (int i = 0; i < slots.length; ++i) {
      // This can be negative, so abs() it.
      final int currIncrement = Math.abs(iterationCounter.getAndIncrement() % slots.length);
      final StorageSlot candidateSlot = slots[currIncrement % slots.length];
      if (candidateSlot.runningTaskId != null) {
        continue;
      }
      candidateSlot.runningTaskId = taskId;
      return candidateSlot;
    }
    throw new ISE("Unable to pick a free slot, this should never happen, slot status [%s].", Arrays.toString(slots));
  }

  public synchronized void returnStorageSlot(StorageSlot slot)
  {
    slot.runningTaskId = null;
  }

  @Nullable
  public synchronized File findExistingTaskDir(String taskId)
  {
    File candidateLocation = null;
    if (baseTaskDirs.length == 1) {
      candidateLocation = new File(baseTaskDirs[0], taskId);
    } else {
      for (File baseTaskDir : baseTaskDirs) {
        File maybeExists = new File(baseTaskDir, taskId);
        if (maybeExists.exists()) {
          candidateLocation = maybeExists;
          break;
        }
      }
    }

    if (candidateLocation != null && candidateLocation.exists()) {
      // task exists at old location, relocate to a "good" slot location and return that.
      final StorageSlot taskSlot = pickStorageSlot(taskId);
      final File pickedLocation = new File(taskSlot.getDirectory(), taskId);
      if (candidateLocation.renameTo(pickedLocation)) {
        taskSlot.runningTaskId = taskId;
        return pickedLocation;
      } else {
        throw new ISE("Unable to relocate task ([%s] -> [%s])", candidateLocation, pickedLocation);
      }
    } else {
      for (StorageSlot slot : slots) {
        candidateLocation = new File(slot.getDirectory(), taskId);
        if (candidateLocation.exists()) {
          slot.runningTaskId = taskId;
          return candidateLocation;
        }
      }

      return null;
    }
  }

  public static class StorageSlot
  {
    private final File directory;
    private final long numBytes;

    private volatile String runningTaskId = null;

    private StorageSlot(File baseDirectory, long numBytes)
    {
      this.directory = baseDirectory;
      this.numBytes = numBytes;
    }

    public File getDirectory()
    {
      return directory;
    }

    public long getNumBytes()
    {
      return numBytes;
    }

    @Override
    public String toString()
    {
      return "StorageSlot{" +
             "directory=" + directory +
             ", numBytes=" + numBytes +
             ", runningTaskId='" + runningTaskId + '\'' +
             '}';
    }
  }
}
