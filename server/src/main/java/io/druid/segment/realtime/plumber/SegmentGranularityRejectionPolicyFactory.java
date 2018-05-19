/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.plumber;

import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.JodaUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentGranularityRejectionPolicyFactory implements RejectionPolicyFactory
{
  private static final Logger logger = LoggerFactory.getLogger(SegmentGranularityRejectionPolicyFactory.class);
  private static final DateTimes.UtcFormatter FMT = DateTimes.wrapFormatter(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));

  @Override
  public RejectionPolicy create(final Period windowPeriod, final Granularity segmentGranularity)
  {
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    final long segmentSize = segmentGranularity.bucket(DateTimes.nowUtc()).toDurationMillis();

    return new RejectionPolicy()
    {
      private volatile long maxTimestamp = JodaUtils.MIN_INSTANT;
      private long deadline = JodaUtils.MIN_INSTANT;
      private long segmentEnd = 0;
      private long rejectedCount = 0;

      @Override
      public DateTime getCurrMaxTime()
      {
        return DateTimes.utc(maxTimestamp);
      }

      @Override
      public boolean accept(long timestamp)
      {
        long curTime = System.currentTimeMillis();
        boolean isAccepted = doAccept(timestamp, curTime);
        if (!isAccepted) {
          rejectedCount++;
          if (rejectedCount % 10000 == 0) {
            logger.warn("rejected " + rejectedCount + " messages, msg time:" + formatTime(timestamp)
                        + ", max time:" + formatTime(maxTimestamp));
          }
        }
        return isAccepted;
      }

      private boolean doAccept(long timestamp, long curTime)
      {
        if (timestamp > curTime) {
          if (segmentEnd == 0) {
            long bucketStart = segmentGranularity.bucketStart(DateTimes.utc(curTime)).getMillis();
            long maxSegmentEndAllowed = bucketStart + segmentSize;
            // throw away if timestamp too much ahead current time
            if (timestamp > maxSegmentEndAllowed) {
              return false;
            }
          }
          // throw away if timestamp too much ahead current time
          if (segmentEnd > 0 && timestamp > segmentEnd) {
            return false;
          }
          // if timestamp is little ahead current time, threat it as current time
          timestamp = curTime;
        }
        if (maxTimestamp < timestamp) {
          maxTimestamp = timestamp;
          long bucketStart = segmentGranularity.bucketStart(DateTimes.utc(maxTimestamp)).getMillis();
          segmentEnd = bucketStart + segmentSize;
          deadline = maxTimestamp <= bucketStart + windowMillis ? bucketStart - segmentSize : bucketStart;
        }
        return timestamp >= deadline;
      }

      private String formatTime(long timestamp)
      {
        return FMT.print(DateTimes.utc(timestamp));
      }

      @Override
      public String toString()
      {
        return StringUtils.format("SegmentGranularity-%s", windowPeriod);
      }
    };
  }
}

