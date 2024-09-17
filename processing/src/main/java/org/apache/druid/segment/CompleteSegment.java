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

package org.apache.druid.segment;

import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Contains the {@link DataSegment} and {@link Segment}. The datasegment could be null if the segment is a dummy, such
 * as those created by {@link org.apache.druid.msq.input.inline.InlineInputSliceReader}.
 */
public class CompleteSegment implements Closeable
{
  @Nullable
  private final DataSegment dataSegment;
  private final Segment segment;

  public CompleteSegment(@Nullable DataSegment dataSegment, Segment segment)
  {
    this.dataSegment = dataSegment;
    this.segment = segment;
  }

  @Nullable
  @SuppressWarnings("unused")
  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  public Segment getSegment()
  {
    return segment;
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
    CompleteSegment that = (CompleteSegment) o;
    return Objects.equals(dataSegment, that.dataSegment) && Objects.equals(segment, that.segment);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSegment, segment);
  }

  @Override
  public void close() throws IOException
  {
    segment.close();
  }
}
