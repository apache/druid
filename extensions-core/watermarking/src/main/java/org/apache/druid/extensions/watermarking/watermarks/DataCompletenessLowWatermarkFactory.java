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

package org.apache.druid.extensions.watermarking.watermarks;

import com.google.inject.Inject;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.cache.WatermarkCache;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

public class DataCompletenessLowWatermarkFactory extends WatermarkCursorFactory
{
  @Inject
  public DataCompletenessLowWatermarkFactory(
      WatermarkCache watermarkCache,
      WatermarkSink watermarkSink
  )
  {
    super(watermarkCache, watermarkSink);
  }

  @Override
  public String getGapType()
  {
    return DataCompletenessLowWatermark.TYPE;
  }

  @Override
  public WatermarkCursor build(
      String dataSource,
      Interval interval,
      List<TimelineObjectHolder<String, ServerSelector>> segments,
      boolean isCompleteTimeline
  )
  {
    DataCompletenessLowWatermark watermark = new DataCompletenessLowWatermark(
        cache,
        watermarkSink,
        dataSource,
        interval
    );
    DateTime startDate = segments.get(0).getInterval().getStart();
    if (isCompleteTimeline
        || startDate.isEqual(interval.getStart())
        || startDate.isEqual(watermark.getWatermark())
        || startDate.isBefore(watermark.getWatermark())) {
      return watermark;
    }
    return null;
  }

  @Override
  public boolean canOverride()
  {
    return true;
  }

  @Override
  public void manualOverride(String dataSource, DateTime timestamp)
  {
    DataCompletenessLowWatermark wm =
        new DataCompletenessLowWatermark(cache, watermarkSink, dataSource, null);

    wm.manualOverride(timestamp);
  }

  @Override
  public Watermark getWatermark(String dataSource)
  {
    return new DataCompletenessLowWatermark(cache, watermarkSink, dataSource, null);
  }
}
