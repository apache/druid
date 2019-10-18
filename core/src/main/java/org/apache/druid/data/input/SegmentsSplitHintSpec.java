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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * {@link SplitHintSpec} for IngestSegmentFirehoseFactory.
 */
public class SegmentsSplitHintSpec implements SplitHintSpec
{
  public static final String TYPE = "segments";

  private static final long DEFAULT_MAX_INPUT_SEGMENT_BYTES_PER_TASK = 500 * 1024 * 1024;

  /**
   * Maximum number of bytes of input segments to process in a single task.
   * If a single segment is larger than this number, it will be processed by itself in a single task.
   */
  private final long maxInputSegmentBytesPerTask;

  @JsonCreator
  public SegmentsSplitHintSpec(
      @JsonProperty("maxInputSegmentBytesPerTask") @Nullable Long maxInputSegmentBytesPerTask
  )
  {
    this.maxInputSegmentBytesPerTask = maxInputSegmentBytesPerTask == null
                                       ? DEFAULT_MAX_INPUT_SEGMENT_BYTES_PER_TASK
                                       : maxInputSegmentBytesPerTask;
  }

  @JsonProperty
  public long getMaxInputSegmentBytesPerTask()
  {
    return maxInputSegmentBytesPerTask;
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
    SegmentsSplitHintSpec that = (SegmentsSplitHintSpec) o;
    return maxInputSegmentBytesPerTask == that.maxInputSegmentBytesPerTask;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxInputSegmentBytesPerTask);
  }

  @Override
  public String toString()
  {
    return "SegmentsSplitHintSpec{" +
           "maxInputSegmentBytesPerTask=" + maxInputSegmentBytesPerTask +
           '}';
  }
}
