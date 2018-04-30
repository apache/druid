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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public class SegmentWithState
{
  /**
   * Segment state transition is different in {@link BatchAppenderatorDriver} and {@link StreamAppenderatorDriver}.
   * When a new segment is created, its state is {@link #APPENDING}.
   *
   * - In stream ingestion, the state of some segments can be changed to the {@link #APPEND_FINISHED} state. Data is
   * not appended to these segments anymore, and they are waiting for beging published.
   * See {@link StreamAppenderatorDriver#moveSegmentOut(String, List)}.
   * - In batch ingestion, the state of some segments can be changed to the {@link #PUSHED_AND_DROPPED} state. These
   * segments are pushed and dropped from the local storage, but not published yet.
   * See {@link BatchAppenderatorDriver#pushAndClear(Collection, long)}.
   *
   * Note: If you need to add more states which are used differently in batch and streaming ingestion, consider moving
   * SegmentState to {@link BatchAppenderatorDriver} and {@link StreamAppenderatorDriver}.
   */
  public enum SegmentState
  {
    APPENDING,
    APPEND_FINISHED,   // only used in StreamAppenderatorDriver
    PUSHED_AND_DROPPED; // only used in BatchAppenderatorDriver

    @JsonCreator
    public static SegmentState fromString(@JsonProperty String name)
    {
      if (name.equalsIgnoreCase("ACTIVE")) {
        return APPENDING;
      } else if (name.equalsIgnoreCase("INACTIVE")) {
        return APPEND_FINISHED;
      } else {
        return SegmentState.valueOf(name);
      }
    }
  }

  private final SegmentIdentifier segmentIdentifier;
  private SegmentState state;

  /**
   * This is to keep what dataSegment object was created for {@link #segmentIdentifier} when
   * {@link BaseAppenderatorDriver#pushInBackground} is called.
   */
  @Nullable private DataSegment dataSegment;

  static SegmentWithState newSegment(SegmentIdentifier segmentIdentifier)
  {
    return new SegmentWithState(segmentIdentifier, SegmentState.APPENDING, null);
  }

  static SegmentWithState newSegment(SegmentIdentifier segmentIdentifier, SegmentState state)
  {
    return new SegmentWithState(segmentIdentifier, state, null);
  }

  @JsonCreator
  public SegmentWithState(
      @JsonProperty("segmentIdentifier") SegmentIdentifier segmentIdentifier,
      @JsonProperty("state") SegmentState state,
      @JsonProperty("dataSegment") @Nullable DataSegment dataSegment)
  {
    this.segmentIdentifier = segmentIdentifier;
    this.state = state;
    this.dataSegment = dataSegment;
  }

  public void setState(SegmentState state)
  {
    this.state = state;
  }

  /**
   * Change the segment state to {@link SegmentState#APPEND_FINISHED}. The current state should be
   * {@link SegmentState#APPENDING}.
   */
  public void finishAppending()
  {
    checkStateTransition(this.state, SegmentState.APPENDING, SegmentState.APPEND_FINISHED);
    this.state = SegmentState.APPEND_FINISHED;
  }

  /**
   * Change the segment state to {@link SegmentState#PUSHED_AND_DROPPED}. The current state should be
   * {@link SegmentState#APPENDING}. This method should be called after the segment of {@link #segmentIdentifier} is
   * completely pushed and dropped.
   *
   * @param dataSegment pushed {@link DataSegment}
   */
  public void pushAndDrop(DataSegment dataSegment)
  {
    checkStateTransition(this.state, SegmentState.APPENDING, SegmentState.PUSHED_AND_DROPPED);
    this.state = SegmentState.PUSHED_AND_DROPPED;
    this.dataSegment = dataSegment;
  }

  @JsonProperty
  public SegmentIdentifier getSegmentIdentifier()
  {
    return segmentIdentifier;
  }

  @JsonProperty
  public SegmentState getState()
  {
    return state;
  }

  @JsonProperty
  @Nullable
  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  private static void checkStateTransition(SegmentState actualFrom, SegmentState expectedFrom, SegmentState to)
  {
    Preconditions.checkState(
        actualFrom == expectedFrom,
        "Wrong state transition from [%s] to [%s]",
        actualFrom,
        to
    );
  }

  @Override
  public String toString()
  {
    return "SegmentWithState{" +
           "segmentIdentifier=" + segmentIdentifier +
           ", state=" + state +
           '}';
  }
}
