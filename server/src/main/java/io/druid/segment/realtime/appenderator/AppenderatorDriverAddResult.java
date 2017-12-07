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
 * Result of {@link AppenderatorDriver#add(InputRow, String, Supplier, boolean)}.  It contains the identifier of the
 * segment which the InputRow is added to, the number of rows in that segment and if persist is required because either
 * maxRowsInMemory or intermediate persist period threshold is hit.
 */
public class AppenderatorDriverAddResult
{
  private final SegmentIdentifier segmentIdentifier;
  private final int numRowsInSegment;
  private final long totalNumRowsInAppenderator;
  private final boolean isPersistRequired;

  public static AppenderatorDriverAddResult ok(
      SegmentIdentifier segmentIdentifier,
      int numRowsInSegment,
      long totalNumRowsInAppenderator,
      boolean isPersistRequired
  )
  {
    return new AppenderatorDriverAddResult(segmentIdentifier, numRowsInSegment, totalNumRowsInAppenderator, isPersistRequired);
  }

  public static AppenderatorDriverAddResult fail()
  {
    return new AppenderatorDriverAddResult(null, 0, 0, false);
  }

  private AppenderatorDriverAddResult(
      @Nullable SegmentIdentifier segmentIdentifier,
      int numRowsInSegment,
      long totalNumRowsInAppenderator,
      boolean isPersistRequired
  )
  {
    this.segmentIdentifier = segmentIdentifier;
    this.numRowsInSegment = numRowsInSegment;
    this.totalNumRowsInAppenderator = totalNumRowsInAppenderator;
    this.isPersistRequired = isPersistRequired;
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

  public boolean isPersistRequired()
  {
    return isPersistRequired;
  }
}
