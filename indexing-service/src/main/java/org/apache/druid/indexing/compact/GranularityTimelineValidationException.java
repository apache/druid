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

package org.apache.druid.indexing.compact;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;

/**
 * Exception thrown when segment granularity timeline validation fails when using {@link CascadingReindexingTemplate}.
 * Contains structured information about the conflicting intervals.
 */
public class GranularityTimelineValidationException extends IAE
{
  private final Interval olderInterval;
  private final Granularity olderGranularity;
  private final Interval newerInterval;
  private final Granularity newerGranularity;

  public GranularityTimelineValidationException(
      String dataSource,
      Interval olderInterval,
      Granularity olderGranularity,
      Interval newerInterval,
      Granularity newerGranularity
  )
  {
    super(
        "Invalid segment granularity timeline for dataSource[%s]: "
        + "Interval[%s] with granularity[%s] is more recent than "
        + "interval[%s] with granularity[%s], but has a coarser granularity. "
        + "Segment granularity must stay the same or become finer as data ages from present to past.",
        dataSource,
        newerInterval,
        newerGranularity,
        olderInterval,
        olderGranularity
    );
    this.olderInterval = olderInterval;
    this.olderGranularity = olderGranularity;
    this.newerInterval = newerInterval;
    this.newerGranularity = newerGranularity;
  }

  public Interval getOlderInterval()
  {
    return olderInterval;
  }

  public Granularity getOlderGranularity()
  {
    return olderGranularity;
  }

  public Interval getNewerInterval()
  {
    return newerInterval;
  }

  public Granularity getNewerGranularity()
  {
    return newerGranularity;
  }
}
