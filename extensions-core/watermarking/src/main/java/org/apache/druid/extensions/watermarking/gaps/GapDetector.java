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

package org.apache.druid.extensions.watermarking.gaps;

import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCursor;
import org.apache.druid.extensions.timeline.metadata.TimelineSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

public abstract class GapDetector implements TimelineMetadataCursor<List<Interval>>
{
  protected final String datasource;
  protected final Interval interval;
  protected List<Interval> gaps;
  protected DateTime cursorTimestamp;

  public GapDetector(String datasource, Interval interval)
  {
    this.datasource = datasource;
    this.interval = interval;
    reset();
  }

  @Override
  public void reset()
  {
    this.cursorTimestamp = interval.getStart();
    this.gaps = new ArrayList<>();
  }

  @Override
  public boolean complete()
  {
    return true;
  }

  @Override
  public String getDataSource()
  {
    return datasource;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public DateTime getTimestamp()
  {
    return null;
  }

  @Override
  public List<Interval> getMetadata()
  {
    return gaps;
  }

  protected boolean isContiguous(TimelineSegment segmentDetails)
  {
    return segmentDetails.isContiguous(cursorTimestamp);
  }
}
