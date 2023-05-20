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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Class that periodically checks with the coordinator if all the segments generated are loaded and
 * blocks till it is complete. This will account and not wait for segments that would never be loaded due to
 * load rules. Only used if the query generates new segments or tombstones.
 * <br>
 * If an exception is thrown during operation, this will simply log the exception and exit without failing the task,
 * since the segments have already been published successfully, and should be loaded eventually.
 * <br>
 * If the segments are not loaded by {@link #DEFAULT_WAIT_TIMEOUT}, logs a warning and exits.
 */
public class SegmentLoadAwaiter
{
  private static final Logger log = new Logger(SegmentLoadAwaiter.class);
  private static final long DEFAULT_WAIT_TIMEOUT = TimeUnit.MINUTES.toMillis(10);
  private static final long SLEEP_DURATION = TimeUnit.SECONDS.toMillis(15);

  private final CoordinatorClient coordinatorClient;
  private final String dataSource;
  private final Set<SegmentDescriptor> segmentsToAwait;
  private volatile Status status;

  public SegmentLoadAwaiter(CoordinatorClient coordinatorClient, String dataSource, Set<DataSegment> dataSegments)
  {
    this.coordinatorClient = coordinatorClient;
    this.dataSource = dataSource;
    this.segmentsToAwait = dataSegments.stream()
                                       .map(DataSegment::toDescriptor)
                                       .collect(Collectors.toSet());
  }

  /**
   * Uses coordinator client to check if all segments in {@link #segmentsToAwait} have finished handing off. Hand off
   * is when a segment has finished loading, and immedietly returns true if it would not be loaded according to the
   * load rules.
   * <br>
   * If an exception is thrown during operation, this will log the exception and return without failing the task,
   * since the segments have already been published successfully, and should be loaded eventually.
   * <br>
   * Only expected to be called from the main controller thread.
   */
  public void awaitSegmentLoad()
  {
    int totalSegments = segmentsToAwait.size();
    DateTime startTime = DateTime.now();

    try {
      while (!segmentsToAwait.isEmpty()) {

        long runningMillis = new Interval(startTime, DateTime.now()).toDurationMillis();
        if (runningMillis > DEFAULT_WAIT_TIMEOUT) {
          log.warn("Timeout %s exceeded while waiting for segments to load. Exiting.", DEFAULT_WAIT_TIMEOUT);
          status = new Status(State.TIMED_OUT, startTime, runningMillis, totalSegments, segmentsToAwait.size());
          return;
        }

        Iterator<SegmentDescriptor> iterator = segmentsToAwait.iterator();
        while (iterator.hasNext()) {
          SegmentDescriptor descriptor = iterator.next();
          Boolean handOffComplete = coordinatorClient.isHandOffComplete(dataSource, descriptor);
          if (Boolean.TRUE.equals(handOffComplete)) {
            iterator.remove();
          }
        }

        if (segmentsToAwait.isEmpty()) {
          break;
        } else {
          // Update the status
          runningMillis = new Interval(startTime, DateTime.now()).toDurationMillis();
          status = new Status(State.RUNNING, startTime, runningMillis, totalSegments, segmentsToAwait.size());

          // Sleep for a while before retrying
          Thread.sleep(SLEEP_DURATION);
        }
      }
    } catch (Exception e) {
      log.warn(e, "Exception occurred while waiting for segments to load. Exiting.");
      long runningMillis = new Interval(startTime, DateTime.now()).toDurationMillis();
      status = new Status(State.FAILED, startTime, runningMillis, totalSegments, segmentsToAwait.size());
    }

    long runningMillis = new Interval(startTime, DateTime.now()).toDurationMillis();
    status = new Status(State.SUCCESS, startTime, runningMillis, totalSegments, segmentsToAwait.size());
  }

  public Status status() {
    return status;
  }

  public static class Status {
    private final State state;
    private final DateTime startTime;
    private final long duration;
    private final int totalSegments;
    private final int segmentsLeft;

    @JsonCreator
    public Status(
        @JsonProperty("state") State state,
        @JsonProperty("startTime") DateTime startTime,
        @JsonProperty("duration") long duration,
        @JsonProperty("totalSegments") int totalSegments,
        @JsonProperty("segmentsLeft") int segmentsLeft
    )
    {
      this.state = state;
      this.startTime = startTime;
      this.duration = duration;
      this.totalSegments = totalSegments;
      this.segmentsLeft = segmentsLeft;
    }

    @JsonProperty
    public State getState()
    {
      return state;
    }

    @JsonProperty
    public DateTime getStartTime()
    {
      return startTime;
    }

    @JsonProperty
    public long getDuration()
    {
      return duration;
    }

    @JsonProperty
    public long getTotalSegments()
    {
      return totalSegments;
    }

    @JsonProperty
    public int getSegmentsLeft()
    {
      return segmentsLeft;
    }
  }

  public enum State {
    RUNNING,
    SUCCESS,
    FAILED,
    TIMED_OUT
  }
}
