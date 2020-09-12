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

package org.apache.druid.testing.utils;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

import java.util.List;

public abstract class SyntheticStreamGenerator implements StreamGenerator
{
  private static final Logger LOG = new Logger(SyntheticStreamGenerator.class);

  private final EventSerializer serializer;
  private final int eventsPerSecond;

  // When calculating rates, leave this buffer to minimize overruns where we're still writing messages from the previous
  // second. If the generator finishes sending [eventsPerSecond] events and the second is not up, it will wait for the next
  // second to begin.
  private final long cyclePaddingMs;

  public SyntheticStreamGenerator(EventSerializer serializer, int eventsPerSecond, long cyclePaddingMs)
  {
    this.serializer = serializer;
    this.eventsPerSecond = eventsPerSecond;
    this.cyclePaddingMs = cyclePaddingMs;
  }

  abstract List<Pair<String, Object>> newEvent(int row, DateTime timestamp);

  @Override
  public void run(String streamTopic, StreamEventWriter streamEventWriter, int totalNumberOfSeconds)
  {
    run(streamTopic, streamEventWriter, totalNumberOfSeconds, null);
  }

  @Override
  public void run(String streamTopic, StreamEventWriter streamEventWriter, int totalNumberOfSeconds, DateTime overrrideFirstEventTime)
  {
    // The idea here is that we will send [eventsPerSecond] events that will either use [nowFlooredToSecond]
    // or the [overrrideFirstEventTime] as the primary timestamp.
    // Having a fixed number of events that use the same timestamp will help in allowing us to determine if any events
    // were dropped or duplicated. We will try to space the event generation over the remainder of the second so that it
    // roughly completes at the top of the second, but if it doesn't complete, it will still send the remainder of the
    // events with the original timestamp, even after wall time has moved onto the next second.
    DateTime nowCeilingToSecond = DateTimes.nowUtc().secondOfDay().roundCeilingCopy();
    DateTime eventTimestamp = overrrideFirstEventTime == null ? nowCeilingToSecond : overrrideFirstEventTime;
    int seconds = 0;

    while (true) {
      try {
        long sleepMillis = nowCeilingToSecond.getMillis() - DateTimes.nowUtc().getMillis();
        if (sleepMillis > 0) {
          LOG.info("Waiting %s ms for next run cycle (at %s)", sleepMillis, nowCeilingToSecond);
          Thread.sleep(sleepMillis);
          continue;
        }

        LOG.info(
            "Beginning run cycle with %s events, target completion time: %s",
            eventsPerSecond,
            nowCeilingToSecond.plusSeconds(1).minus(cyclePaddingMs)
        );

        if (streamEventWriter.supportTransaction() && streamEventWriter.isTransactionEnabled()) {
          streamEventWriter.initTransaction();
        }

        for (int i = 1; i <= eventsPerSecond; i++) {
          streamEventWriter.write(streamTopic, serializer.serialize(newEvent(i, eventTimestamp)));

          long sleepTime = calculateSleepTimeMs(eventsPerSecond - i, nowCeilingToSecond);
          if ((i <= 100 && i % 10 == 0) || i % 100 == 0) {
            LOG.info("Event: %s/%s, sleep time: %s ms", i, eventsPerSecond, sleepTime);
          }

          if (sleepTime > 0) {
            Thread.sleep(sleepTime);
          }
        }

        if (streamEventWriter.supportTransaction() && streamEventWriter.isTransactionEnabled()) {
          streamEventWriter.commitTransaction();
        }

        nowCeilingToSecond = nowCeilingToSecond.plusSeconds(1);
        eventTimestamp = eventTimestamp.plusSeconds(1);
        seconds++;

        LOG.info(
            "Finished writing %s events, current time: %s - updating next timestamp to: %s",
            eventsPerSecond,
            DateTimes.nowUtc(),
            nowCeilingToSecond
        );

        if (seconds >= totalNumberOfSeconds) {
          streamEventWriter.flush();
          LOG.info(
              "Finished writing %s seconds",
              seconds
          );
          break;
        }
      }
      catch (Exception e) {
        throw new RuntimeException("Exception in event generation loop", e);
      }
    }
  }

  /**
   * Dynamically adjust delay between messages to spread them out over the remaining time left in the second.
   */
  private long calculateSleepTimeMs(long eventsRemaining, DateTime secondBeingProcessed)
  {
    if (eventsRemaining == 0) {
      return 0;
    }

    DateTime now = DateTimes.nowUtc();
    DateTime nextSecondToProcessMinusBuffer = secondBeingProcessed.plusSeconds(1).minus(cyclePaddingMs);

    if (nextSecondToProcessMinusBuffer.isBefore(now)) {
      return 0; // We're late!! Write messages as fast as you can
    }

    return (nextSecondToProcessMinusBuffer.getMillis() - now.getMillis()) / eventsRemaining;
  }
}
