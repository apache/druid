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

import org.apache.druid.extensions.timeline.metadata.TimelineSegment;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.cache.WatermarkCache;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.Interval;

/**
 * The low watermark of data completeness, which is the most recent timestamp
 * of the largest contiguous timeline of a data source. This is lower bounded
 * by the BatchCompletenessLowWatermark.
 *
 * @see BatchCompletenessLowWatermark
 */
public class DataCompletenessLowWatermark extends LowerBoundWatermarkCursor
{
  public static final String TYPE = "stable_low";
  private static final Logger LOG = new Logger(DataCompletenessLowWatermark.class);

  public DataCompletenessLowWatermark(
      WatermarkCache source,
      WatermarkSink sink,
      String dataSource,
      Interval interval
  )
  {
    super(source, sink, dataSource, interval, BatchCompletenessLowWatermark.TYPE);
    LOG.debug("Created cursor for %s", dataSource);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean advance(TimelineSegment segmentDetails)
  {
    if (isContiguous(segmentDetails) && segmentDetails.getIsSegmentImmutable()) {
      return advanceTimestamp(segmentDetails.getInterval().getEnd());
    }
    if (canSkip(segmentDetails)) {
      LOG.debug(
          "Data completeness cursor detected gap detected at %s behind current effective watermark %s for %s",
          segmentDetails.getInterval().getStart(),
          getMetadata().toString(),
          getDataSource()
      );
      return true;
    }
    return !complete();
  }
}
