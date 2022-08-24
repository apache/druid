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

package org.apache.druid.msq.counters;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.frame.processor.SuperSorterProgressSnapshot;
import org.apache.druid.frame.processor.SuperSorterProgressTracker;

import javax.annotation.Nullable;

public class SuperSorterProgressTrackerCounter implements QueryCounter
{
  private final SuperSorterProgressTracker tracker;

  public SuperSorterProgressTrackerCounter()
  {
    this.tracker = new SuperSorterProgressTracker();
  }

  public SuperSorterProgressTracker tracker()
  {
    return tracker;
  }

  @Nullable
  @Override
  public QueryCounterSnapshot snapshot()
  {
    return new Snapshot(tracker.snapshot());
  }

  /**
   * Wrapper class that exists for JSON serde.
   */
  @JsonTypeName("sortProgress")
  public static class Snapshot implements QueryCounterSnapshot
  {
    private final SuperSorterProgressSnapshot snapshot;

    @JsonCreator
    public Snapshot(SuperSorterProgressSnapshot snapshot)
    {
      this.snapshot = snapshot;
    }

    @JsonValue
    public SuperSorterProgressSnapshot getSnapshot()
    {
      return snapshot;
    }
  }
}
