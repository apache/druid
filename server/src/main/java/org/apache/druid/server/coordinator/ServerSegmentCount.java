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

package org.apache.druid.server.coordinator;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.Map;

public class ServerSegmentCount
{
  private final Map<String, Object2IntOpenHashMap<Interval>>
      datasourceIntervalToSegmentCount = new HashMap<>();

  public void addSegment(DataSegment segment)
  {
    updateCountInInterval(segment, 1);
  }

  public void removeSegment(DataSegment segment)
  {
    updateCountInInterval(segment, -1);
  }

  public Map<String, Object2IntOpenHashMap<Interval>> getDatasourceIntervalToSegmentCount()
  {
    return datasourceIntervalToSegmentCount;
  }

  private void updateCountInInterval(DataSegment segment, int delta)
  {
    datasourceIntervalToSegmentCount
        .computeIfAbsent(segment.getDataSource(), ds -> new Object2IntOpenHashMap<>())
        .addTo(segment.getInterval(), delta);
  }
}
