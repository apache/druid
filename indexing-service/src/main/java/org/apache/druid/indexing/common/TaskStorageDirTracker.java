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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Used to pick storage slots for tasks when run from the middle manager.
 */
public class TaskStorageDirTracker
{
  private static final Logger log = new Logger(TaskStorageDirTracker.class);

  public static TaskStorageDirTracker fromConfigs(WorkerConfig workerConfig, TaskConfig taskConfig)
  {
    final List<String> basePaths = workerConfig.getBaseTaskDirs();
    final List<File> baseTaskDirs;

    if (basePaths == null) {
      baseTaskDirs = ImmutableList.of(taskConfig.getBaseTaskDir());
    } else {
      baseTaskDirs = basePaths.stream().map(File::new).collect(Collectors.toList());
    }

    return fromBaseDirs(baseTaskDirs, workerConfig.getCapacity(), workerConfig.getBaseTaskDirSize());
  }

  public static TaskStorageDirTracker fromBaseDirs(List<File> baseTaskDirs, int numSlots, long dirSize)
  {
    int slotsPerBaseTaskDir = numSlots / baseTaskDirs.size();
    if (slotsPerBaseTaskDir == 0) {
      slotsPerBaseTaskDir = 1;
    } else if (numSlots % baseTaskDirs.size() > 0) {
      // We have to add an extra slot per location if they do not evenly divide
      ++slotsPerBaseTaskDir;
    }
    long sizePerSlot = dirSize / slotsPerBaseTaskDir;

    File[] slotDirs = new File[numSlots];
    for (int i = 0; i < numSlots; ++i) {
      final int whichDir = i % baseTaskDirs.size();
      final int dirUsageCount = i / baseTaskDirs.size();
      slotDirs[i] = new File(baseTaskDirs.get(whichDir), StringUtils.format("slot%d", dirUsageCount));
    }

    return new TaskStorageDirTracker(baseTaskDirs, slotDirs, sizePerSlot);
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

  private TaskStorageDirTracker(List<File> baseTaskDirs, File[] slotDirs, long sizePerSlot)
  {
    this.baseTaskDirs = baseTaskDirs.toArray(new File[0]);
    this.slots = new StorageSlot[slotDirs.length];
    for (int i = 0; i < slotDirs.length; ++i) {
      slots[i] = new StorageSlot(slotDirs[i], sizePerSlot);
    }
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
      if (candidateSlot.runningTaskId == null) {
        candidateSlot.runningTaskId = taskId;
        return candidateSlot;
      }
    }
    throw new ISE("Unable to pick a free slot, this should never happen, slot status [%s].", Arrays.toString(slots));
  }

  public synchronized void returnStorageSlot(StorageSlot slot)
  {
    if (slot.getParentRef() == this) {
      slot.runningTaskId = null;
    } else {
      throw new IAE("Cannot return storage slot for task [%s] that I don't own.", slot.runningTaskId);
    }
  }

  /**
   * Finds directories that might already exist for the list of tasks.  Useful in restoring tasks upon restart.
   *
   * @param taskIds the ids to find and restore
   * @return a map of taskId to the StorageSlot for that task.  Contains null values for ids that couldn't be found
   */
  public synchronized Map<String, StorageSlot> findExistingTaskDirs(List<String> taskIds)
  {
    // Use a tree map because we don't expect this to be too large, but it's nice to have the keys sorted
    // if this ever gets printed out.
    Map<String, StorageSlot> retVal = new TreeMap<>();
    List<String> missingIds = new ArrayList<>();


    // We need to start by looking for the tasks in current slot locations so that we ensure that we have
    // correct in-memory accounting for anything that is currently running in a known slot.  After that, for
    // compatibility with an old implementation, we need to check the base directories to see if any of
    // the tasks are running in the legacy locations and assign them to one of the free task slots.
    for (String taskId : taskIds) {
      StorageSlot candidateSlot = Arrays.stream(slots)
                                        .filter(slot -> slot.runningTaskId == null)
                                        .filter(slot -> new File(slot.getDirectory(), taskId).exists())
                                        .findFirst()
                                        .orElse(null);

      if (candidateSlot == null) {
        missingIds.add(taskId);
      } else {
        candidateSlot.runningTaskId = taskId;
        retVal.put(taskId, candidateSlot);
      }
    }

    for (String missingId : missingIds) {
      File definitelyExists = null;
      for (File baseTaskDir : baseTaskDirs) {
        File maybeExists = new File(baseTaskDir, missingId);
        if (maybeExists.exists()) {
          definitelyExists = maybeExists;
          break;
        }
      }

      if (definitelyExists == null) {
        retVal.put(missingId, null);
      } else {
        final StorageSlot pickedSlot = pickStorageSlot(missingId);
        final File pickedLocation = new File(pickedSlot.getDirectory(), missingId);
        if (definitelyExists.renameTo(pickedLocation)) {
          retVal.put(missingId, pickedSlot);
        } else {
          log.warn("Unable to relocate task ([%s] -> [%s]), pretend it didn't exist", definitelyExists, pickedLocation);
          retVal.put(missingId, null);
          returnStorageSlot(pickedSlot);
        }
      }
    }

    return retVal;
  }

  public class StorageSlot
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

    public TaskStorageDirTracker getParentRef()
    {
      return TaskStorageDirTracker.this;
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
