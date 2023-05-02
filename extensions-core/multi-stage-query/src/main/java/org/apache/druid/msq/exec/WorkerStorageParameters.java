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

package org.apache.druid.msq.exec;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.NotEnoughTemporaryStorageFault;

import java.util.Objects;

/**
 * Class for determining the amount of temporary disk space to allocate to various purposes, given the per-worker limit.
 * Similar to {@link WorkerMemoryParameters}, but for temporary disk space.
 *
 * Currently only used to allocate disk space for intermediate output from super sorter storage, if intermediate super
 * sorter storage is enabled.
 *
 * If it is enabled, keeps {@link #MINIMUM_BASIC_OPERATIONS_BYTES} for miscellaneous operations and
 * configures the super sorter to use {@link #SUPER_SORTER_TMP_STORAGE_USABLE_FRACTION} of the remaining space for
 * intermediate files. If this value is less than {@link #MINIMUM_SUPER_SORTER_TMP_STORAGE_BYTES},
 * {@link NotEnoughTemporaryStorageFault} is thrown.
 */
public class WorkerStorageParameters
{
  /**
   * Fraction of temporary worker storage that can be allocated to super sorter intermediate files.
   */
  private static final double SUPER_SORTER_TMP_STORAGE_USABLE_FRACTION = 0.8;

  /**
   * Fixed amount of temporary disc storage reserved for miscellaneous operations.
   */
  private static final long MINIMUM_BASIC_OPERATIONS_BYTES = 1_000_000_000L;

  /**
   * Minimum threshold for number of bytes required for intermediate files. If the number of bytes is less than this
   * threshold and intermediate super sorter storage is enabled, {@link NotEnoughTemporaryStorageFault} is thrown.
   */
  private static final long MINIMUM_SUPER_SORTER_TMP_STORAGE_BYTES = 1_000_000_000L;

  private final long intermediateSuperSorterStorageMaxLocalBytes;

  private WorkerStorageParameters(final long intermediateSuperSorterStorageMaxLocalBytes)
  {
    this.intermediateSuperSorterStorageMaxLocalBytes = intermediateSuperSorterStorageMaxLocalBytes;
  }

  public static WorkerStorageParameters createProductionInstance(
      final Injector injector,
      final boolean isIntermediateSuperSorterStorageEnabled
  )
  {
    long tmpStorageBytesPerTask = injector.getInstance(TaskConfig.class).getTmpStorageBytesPerTask();
    return createInstance(tmpStorageBytesPerTask, isIntermediateSuperSorterStorageEnabled);
  }

  @VisibleForTesting
  public static WorkerStorageParameters createInstanceForTests(final long tmpStorageBytesPerTask)
  {
    return new WorkerStorageParameters(tmpStorageBytesPerTask);
  }

  /**
   * Returns an object specifiying temporary disk-usage parameters
   * @param tmpStorageBytesPerTask                  amount of disk space to be allocated per task for intermediate files.
   * @param isIntermediateSuperSorterStorageEnabled whether intermediate super sorter storage is enabled
   */
  public static WorkerStorageParameters createInstance(
      final long tmpStorageBytesPerTask,
      final boolean isIntermediateSuperSorterStorageEnabled
  )
  {
    if (!isIntermediateSuperSorterStorageEnabled || tmpStorageBytesPerTask == -1) {
      return new WorkerStorageParameters(-1);
    }

    Preconditions.checkArgument(tmpStorageBytesPerTask > 0, "Temporary storage bytes passed: [%s] should be > 0", tmpStorageBytesPerTask);
    long intermediateSuperSorterStorageMaxLocalBytes = computeUsableStorage(tmpStorageBytesPerTask);

    if (intermediateSuperSorterStorageMaxLocalBytes < MINIMUM_SUPER_SORTER_TMP_STORAGE_BYTES) {
      throw new MSQException(
          new NotEnoughTemporaryStorageFault(
              calculateSuggestedMinTemporaryStorage(),
              tmpStorageBytesPerTask
          )
      );
    }

    return new WorkerStorageParameters(intermediateSuperSorterStorageMaxLocalBytes);
  }

  private static long computeUsableStorage(long tmpStorageBytesPerTask)
  {
    return (long) (SUPER_SORTER_TMP_STORAGE_USABLE_FRACTION * (tmpStorageBytesPerTask - MINIMUM_BASIC_OPERATIONS_BYTES));
  }

  private static long calculateSuggestedMinTemporaryStorage()
  {
    return MINIMUM_BASIC_OPERATIONS_BYTES + (long) (MINIMUM_SUPER_SORTER_TMP_STORAGE_BYTES / SUPER_SORTER_TMP_STORAGE_USABLE_FRACTION);
  }

  public long getIntermediateSuperSorterStorageMaxLocalBytes()
  {
    return intermediateSuperSorterStorageMaxLocalBytes;
  }

  public boolean isIntermediateStorageLimitConfigured()
  {
    return intermediateSuperSorterStorageMaxLocalBytes != -1;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerStorageParameters that = (WorkerStorageParameters) o;
    return intermediateSuperSorterStorageMaxLocalBytes == that.intermediateSuperSorterStorageMaxLocalBytes;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(intermediateSuperSorterStorageMaxLocalBytes);
  }

  @Override
  public String toString()
  {
    return "WorkerStorageParameters{" +
           "intermediateSuperSorterStorageMaxLocalBytes=" + intermediateSuperSorterStorageMaxLocalBytes +
           '}';
  }
}
