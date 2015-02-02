/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.druid.timeline.DataSegment;

/**
 */
public class SegmentChangeRequestDrop implements DataSegmentChangeRequest
{
  private final DataSegment segment;

  @JsonCreator
  public SegmentChangeRequestDrop(
      @JsonUnwrapped DataSegment segment
  )
  {
    this.segment = segment;
  }

  @JsonProperty
  @JsonUnwrapped
  public DataSegment getSegment()
  {
    return segment;
  }

  @Override
  public void go(DataSegmentChangeHandler handler, DataSegmentChangeCallback callback)
  {
    handler.removeSegment(segment, callback);
  }

  @Override
  public String asString()
  {
    return String.format("DROP: %s", segment.getIdentifier());
  }

  @Override
  public String toString()
  {
    return "SegmentChangeRequestDrop{" +
           "segment=" + segment +
           '}';
  }
}
