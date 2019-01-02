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
 * The high watermark for batch processed data, which is defined as the highest segment
 * boundary that has been processed by batch indexing
 */
public class BatchDataHighWatermark extends HighDataWatermarkCursor
{
  public static final String TYPE = "batch_high";
  private static final Logger log = new Logger(BatchCompletenessLowWatermark.class);

  public BatchDataHighWatermark(
      WatermarkCache source,
      WatermarkSink sink,
      String dataSource,
      Interval interval
  )
  {
    super(source, sink, dataSource, interval, BatchCompletenessLowWatermark.TYPE);
    log.debug("Created cursor for %s", dataSource);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isSegmentAppropriate(TimelineSegment segmentDetails)
  {
    return segmentDetails.getIsBatchIndexed();
  }
}
