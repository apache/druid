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

package org.apache.druid.msq.indexing;

import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.batch.parallel.TombstoneHelper;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MSQTombstoneHelper
{
  private final List<Interval> intervalsToDrop;
  private final List<Interval> intervalsToReplace;
  private final String dataSource;
  private final TaskActionClient taskActionClient;
  private final Granularity replaceGranularity;

  /**
   * @param intervalsToDrop Empty intervals in the query that need to be dropped
   * @param intervalsToReplace Intervals in the query which are eligible for replacement with new data
   * @param dataSource Datasource on which the replace is to be performed
   * @param taskActionClient Task action client
   * @param replaceGranularity Granularity of the replace query
   */
  public MSQTombstoneHelper(
      List<Interval> intervalsToDrop,
      List<Interval> intervalsToReplace,
      String dataSource,
      TaskActionClient taskActionClient,
      Granularity replaceGranularity
  )
  {
    this.intervalsToDrop = intervalsToDrop;
    this.intervalsToReplace = intervalsToReplace;
    this.dataSource = dataSource;
    this.taskActionClient = taskActionClient;
    this.replaceGranularity = replaceGranularity;
  }

  public Set<DataSegment> computeTombstones() throws IOException
  {
    TombstoneHelper tombstoneHelper = new TombstoneHelper(taskActionClient);
    Set<Interval> tombstoneIntervals = tombstoneHelper.computeTombstoneIntervalsForReplace(
        intervalsToReplace,
        intervalsToDrop,
        dataSource,
        replaceGranularity
    );
    Set<DataSegment> tombstones = new HashSet<>();
    for (Interval tombstoneInterval : tombstoneIntervals) {

      final List<TaskLock> locks = taskActionClient.submit(new LockListAction());
      String version = null;
      for (final TaskLock lock : locks) {
        if (lock.getInterval().contains(tombstoneInterval)) {
          version = lock.getVersion();
        }
      }

      if (version == null) {
        // Lock was revoked, probably, because we should have originally acquired it in isReady.
        throw new MSQException(InsertLockPreemptedFault.INSTANCE);
      }

      DataSegment tombstone = TombstoneHelper.createTombstoneForTimeChunkInterval(
          dataSource,
          version,
          new TombstoneShardSpec(),
          tombstoneInterval
      );
      tombstones.add(tombstone);
    }
    return tombstones;
  }
}
