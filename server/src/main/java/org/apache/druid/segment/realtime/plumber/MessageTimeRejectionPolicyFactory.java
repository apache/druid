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

package org.apache.druid.segment.realtime.plumber;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class MessageTimeRejectionPolicyFactory implements RejectionPolicyFactory
{
  @Override
  public RejectionPolicy create(final Period windowPeriod)
  {
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    return new MessageTimeRejectionPolicy(windowMillis, windowPeriod);
  }

  private static class MessageTimeRejectionPolicy implements RejectionPolicy
  {
    private static final AtomicLongFieldUpdater<MessageTimeRejectionPolicy> MAX_TIMESTAMP_UPDATER =
        AtomicLongFieldUpdater.newUpdater(MessageTimeRejectionPolicy.class, "maxTimestamp");
    private final long windowMillis;
    private final Period windowPeriod;
    private volatile long maxTimestamp;

    public MessageTimeRejectionPolicy(long windowMillis, Period windowPeriod)
    {
      this.windowMillis = windowMillis;
      this.windowPeriod = windowPeriod;
      this.maxTimestamp = JodaUtils.MIN_INSTANT;
    }

    @Override
    public DateTime getCurrMaxTime()
    {
      return DateTimes.utc(maxTimestamp);
    }

    @Override
    public boolean accept(long timestamp)
    {
      long maxTimestamp = this.maxTimestamp;
      if (timestamp > maxTimestamp) {
        maxTimestamp = tryUpdateMaxTimestamp(timestamp);
      }

      return timestamp >= (maxTimestamp - windowMillis);
    }

    private long tryUpdateMaxTimestamp(long timestamp)
    {
      long currentMaxTimestamp;
      do {
        currentMaxTimestamp = maxTimestamp;
        if (timestamp <= currentMaxTimestamp) {
          return currentMaxTimestamp;
        }
      } while (!MAX_TIMESTAMP_UPDATER.compareAndSet(this, currentMaxTimestamp, timestamp));
      return timestamp;
    }

    @Override
    public String toString()
    {
      return StringUtils.format("messageTime-%s", windowPeriod);
    }
  }
}

