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

package org.apache.druid.server.coordinator;

public class CompactionStatistics
{
  private long byteSum;
  private long segmentNumberCountSum;
  private long segmentIntervalCountSum;

  public CompactionStatistics(
      long byteSum,
      long segmentNumberCountSum,
      long segmentIntervalCountSum
  )
  {
    this.byteSum = byteSum;
    this.segmentNumberCountSum = segmentNumberCountSum;
    this.segmentIntervalCountSum = segmentIntervalCountSum;
  }

  public static CompactionStatistics initializeCompactionStatistics()
  {
    return new CompactionStatistics(0, 0, 0);
  }

  public long getByteSum()
  {
    return byteSum;
  }

  public long getSegmentNumberCountSum()
  {
    return segmentNumberCountSum;
  }

  public long getSegmentIntervalCountSum()
  {
    return segmentIntervalCountSum;
  }

  public void incrementCompactedByte(long incrementValue)
  {
    byteSum = byteSum + incrementValue;
  }

  public void incrementCompactedSegments(long incrementValue)
  {
    segmentNumberCountSum = segmentNumberCountSum + incrementValue;
  }

  public void incrementCompactedIntervals(long incrementValue)
  {
    segmentIntervalCountSum = segmentIntervalCountSum + incrementValue;
  }
}
