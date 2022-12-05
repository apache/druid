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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;

import java.util.Map;

/**
 * Tree of {@link CounterSnapshots} (named counter snapshots) organized by stage and worker.
 *
 * These are used for worker-to-controller counters propagation with
 * {@link ControllerClient#postCounters} and reporting to end users with
 * {@link MSQTaskReportPayload#getCounters}).
 *
 * The map is mutable, but thread-safe. The individual snapshot objects are immutable.
 */
public class CounterSnapshotsTree
{
  // stage -> worker -> counters
  @GuardedBy("snapshotsMap")
  private final Int2ObjectMap<Int2ObjectMap<CounterSnapshots>> snapshotsMap;

  public CounterSnapshotsTree()
  {
    this.snapshotsMap = new Int2ObjectAVLTreeMap<>();
  }

  @JsonCreator
  public static CounterSnapshotsTree fromMap(final Map<Integer, Map<Integer, CounterSnapshots>> map)
  {
    final CounterSnapshotsTree retVal = new CounterSnapshotsTree();
    retVal.putAll(map);
    return retVal;
  }

  public void put(final int stageNumber, final int workerNumber, final CounterSnapshots snapshots)
  {
    synchronized (snapshotsMap) {
      snapshotsMap.computeIfAbsent(stageNumber, ignored -> new Int2ObjectAVLTreeMap<>())
                  .put(workerNumber, snapshots);
    }
  }

  public void putAll(final CounterSnapshotsTree other)
  {
    putAll(other.copyMap());
  }

  public boolean isEmpty()
  {
    synchronized (snapshotsMap) {
      return snapshotsMap.isEmpty();
    }
  }

  @JsonValue
  public Map<Integer, Map<Integer, CounterSnapshots>> copyMap()
  {
    final Map<Integer, Map<Integer, CounterSnapshots>> retVal = new Int2ObjectAVLTreeMap<>();

    synchronized (snapshotsMap) {
      for (Int2ObjectMap.Entry<Int2ObjectMap<CounterSnapshots>> entry : snapshotsMap.int2ObjectEntrySet()) {
        retVal.put(entry.getIntKey(), new Int2ObjectAVLTreeMap<>(entry.getValue()));
      }
    }

    return retVal;
  }

  private void putAll(final Map<Integer, Map<Integer, CounterSnapshots>> otherMap)
  {
    synchronized (snapshotsMap) {
      for (Map.Entry<Integer, Map<Integer, CounterSnapshots>> stageEntry : otherMap.entrySet()) {
        for (Map.Entry<Integer, CounterSnapshots> workerEntry : stageEntry.getValue().entrySet()) {
          put(stageEntry.getKey(), workerEntry.getKey(), workerEntry.getValue());
        }
      }
    }
  }
}
