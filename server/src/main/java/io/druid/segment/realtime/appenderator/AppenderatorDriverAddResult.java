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

package io.druid.segment.realtime.appenderator;

import com.google.common.base.Supplier;
import io.druid.data.input.InputRow;

import javax.annotation.Nullable;

/**
 * Result of {@link AppenderatorDriver#add(InputRow, String, Supplier)}.  It contains the identifier of the
 * segment which the InputRow is added to and the number of rows in that segment.
 */
public class AppenderatorDriverAddResult
{
  private final SegmentIdentifier segmentIdentifier;
  private final int numRowsInSegment;
  private final long totalNumRowsInAppenderator;

  public static AppenderatorDriverAddResult ok(
      SegmentIdentifier segmentIdentifier,
      int numRowsInSegment,
      long totalNumRowsInAppenderator
  )
  {
    return new AppenderatorDriverAddResult(segmentIdentifier, numRowsInSegment, totalNumRowsInAppenderator);
  }

  public static AppenderatorDriverAddResult fail()
  {
    return new AppenderatorDriverAddResult(null, 0, 0);
  }

  private AppenderatorDriverAddResult(
      @Nullable SegmentIdentifier segmentIdentifier,
      int numRowsInSegment,
      long totalNumRowsInAppenderator
  )
  {
    this.segmentIdentifier = segmentIdentifier;
    this.numRowsInSegment = numRowsInSegment;
    this.totalNumRowsInAppenderator = totalNumRowsInAppenderator;
  }

  public boolean isOk()
  {
    return segmentIdentifier != null;
  }

  public SegmentIdentifier getSegmentIdentifier()
  {
    return segmentIdentifier;
  }

  public int getNumRowsInSegment()
  {
    return numRowsInSegment;
  }

  public long getTotalNumRowsInAppenderator()
  {
    return totalNumRowsInAppenderator;
  }
}
