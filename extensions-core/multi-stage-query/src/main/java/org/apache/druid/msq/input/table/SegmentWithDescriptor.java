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

import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;

import java.io.Closeable;
import java.util.Objects;

public class SegmentWithDescriptor implements Closeable
{
  private final ResourceHolder<? extends Segment> segmentHolder;
  private final SegmentDescriptor descriptor;

  public SegmentWithDescriptor(
      final ResourceHolder<? extends Segment> segmentHolder,
      final SegmentDescriptor descriptor
  )
  {
    this.segmentHolder = Preconditions.checkNotNull(segmentHolder, "segment");
    this.descriptor = Preconditions.checkNotNull(descriptor, "descriptor");
  }

  public Segment getOrLoadSegment()
  {
    return segmentHolder.get();
  }

  @Override
  public void close()
  {
    segmentHolder.close();
  }

  public SegmentDescriptor getDescriptor()
  {
    return descriptor;
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
    SegmentWithDescriptor that = (SegmentWithDescriptor) o;
    return Objects.equals(segmentHolder, that.segmentHolder) && Objects.equals(descriptor, that.descriptor);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentHolder, descriptor);
  }
}
