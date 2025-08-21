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

package org.apache.druid.msq.dart.worker;

import com.google.common.base.Preconditions;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Represents a segment that is queryable at a specific worker number.
 */
public class DartQueryableSegment
{
  private final DataSegment segment;
  private final Interval interval;
  private final int workerNumber;
  @Nullable
  private final DruidServerMetadata realtimeServer;

  public DartQueryableSegment(
      final DataSegment segment,
      final Interval interval,
      final int workerNumber,
      @Nullable DruidServerMetadata realtimeServer
  )
  {
    this.segment = Preconditions.checkNotNull(segment, "segment");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.workerNumber = workerNumber;
    this.realtimeServer = realtimeServer;
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public Interval getInterval()
  {
    return interval;
  }

  public int getWorkerNumber()
  {
    return workerNumber;
  }

  @Nullable
  public DruidServerMetadata getRealtimeServer()
  {
    return realtimeServer;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DartQueryableSegment that = (DartQueryableSegment) o;
    return workerNumber == that.workerNumber
           && Objects.equals(segment, that.segment)
           && Objects.equals(interval, that.interval)
           && Objects.equals(realtimeServer, that.realtimeServer);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segment, interval, workerNumber, realtimeServer);
  }


}
