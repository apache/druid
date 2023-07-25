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

package org.apache.druid.metadata;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Lookup types and parameters for task lookups in the metadata store.
 */
public interface TaskLookup
{
  /**
   * Task state in the metadata store.
   * Complete tasks are the tasks that have been either succeeded or failed.
   * Active tasks are the tasks that are not complete tasks.
   */
  enum TaskLookupType
  {
    ACTIVE,
    COMPLETE
  }

  /**
   * Whether this lookup is guaranteed to not return any tasks.
   */
  boolean isNil();

  TaskLookupType getType();

  /**
   * Task lookup for complete tasks. It includes optional filters for task lookups.
   * When the filters are given, the task lookup returns only the tasks that satisfy all filters.
   */
  class CompleteTaskLookup implements TaskLookup
  {
    /**
     * Limits the number of taskStatuses to return.
     */
    @Nullable
    private final Integer maxTaskStatuses;

    /**
     * Returns only the tasks created prior to the given timestamp.
     */
    @Nullable
    private final DateTime tasksCreatedPriorTo;

    public static CompleteTaskLookup of(
        @Nullable Integer maxTaskStatuses,
        @Nullable Duration durationBeforeNow
    )
    {
      return new CompleteTaskLookup(
          maxTaskStatuses,
          durationBeforeNow == null ? null : computeTimestampPriorToNow(durationBeforeNow)
      );
    }

    @VisibleForTesting
    public static CompleteTaskLookup withTasksCreatedPriorTo(
        @Nullable Integer maxTaskStatuses,
        @Nullable DateTime tasksCreatedPriorTo
    )
    {
      return new CompleteTaskLookup(maxTaskStatuses, tasksCreatedPriorTo);
    }

    public CompleteTaskLookup(
        @Nullable Integer maxTaskStatuses,
        @Nullable DateTime tasksCreatedPriorTo
    )
    {
      this.maxTaskStatuses = maxTaskStatuses;
      this.tasksCreatedPriorTo = tasksCreatedPriorTo;
    }

    public boolean hasTaskCreatedTimeFilter()
    {
      return tasksCreatedPriorTo != null;
    }

    /**
     * If {@link #hasTaskCreatedTimeFilter()}, returns this instance. Otherwise, returns a copy with
     * {@link #getTasksCreatedPriorTo()} based on the provided duration (before now).
     */
    public CompleteTaskLookup withMinTimestampIfAbsent(DateTime minTimestamp)
    {
      if (hasTaskCreatedTimeFilter()) {
        return this;
      } else {
        return new CompleteTaskLookup(maxTaskStatuses, minTimestamp);
      }
    }

    private static DateTime computeTimestampPriorToNow(Duration durationBeforeNow)
    {
      return DateTimes
          .nowUtc()
          .minus(durationBeforeNow);
    }

    public DateTime getTasksCreatedPriorTo()
    {
      assert tasksCreatedPriorTo != null;
      return tasksCreatedPriorTo;
    }

    @Nullable
    public Integer getMaxTaskStatuses()
    {
      return maxTaskStatuses;
    }

    @Override
    public TaskLookupType getType()
    {
      return TaskLookupType.COMPLETE;
    }

    @Override
    public boolean isNil()
    {
      return maxTaskStatuses != null && maxTaskStatuses == 0;
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
      CompleteTaskLookup that = (CompleteTaskLookup) o;
      return Objects.equals(maxTaskStatuses, that.maxTaskStatuses)
             && Objects.equals(tasksCreatedPriorTo, that.tasksCreatedPriorTo);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(maxTaskStatuses, tasksCreatedPriorTo);
    }
  }

  class ActiveTaskLookup implements TaskLookup
  {
    private static final ActiveTaskLookup INSTANCE = new ActiveTaskLookup();

    public static ActiveTaskLookup getInstance()
    {
      return INSTANCE;
    }

    private ActiveTaskLookup()
    {
    }

    @Override
    public TaskLookupType getType()
    {
      return TaskLookupType.ACTIVE;
    }

    @Override
    public boolean isNil()
    {
      return false;
    }

    @Override
    public int hashCode()
    {
      return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof ActiveTaskLookup;
    }
  }
}
