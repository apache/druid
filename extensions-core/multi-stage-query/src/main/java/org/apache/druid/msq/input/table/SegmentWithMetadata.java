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

package org.apache.druid.msq.input.table;

import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.DataSegment;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

public class SegmentWithMetadata implements Closeable
{
  private final DataSegment dataSegment;
  private final Segment segment;

  public SegmentWithMetadata(DataSegment dataSegment, Segment segment)
  {
    this.dataSegment = dataSegment;
    this.segment = segment;
  }

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
    SegmentWithMetadata that = (SegmentWithMetadata) o;
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
