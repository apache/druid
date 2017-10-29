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

package io.druid.query.spec;

import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;

/**
*/
public class SpecificSegmentSpec implements QuerySegmentSpec
{
  private final SegmentDescriptor descriptor;

  public SpecificSegmentSpec(SegmentDescriptor descriptor)
  {
    this.descriptor = descriptor;
  }

  @Override
  public List<Interval> getIntervals()
  {
    return Collections.singletonList(descriptor.getInterval());
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForSegments(query, Collections.singletonList(descriptor));
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

    SpecificSegmentSpec that = (SpecificSegmentSpec) o;

    if (descriptor != null ? !descriptor.equals(that.descriptor) : that.descriptor != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return descriptor != null ? descriptor.hashCode() : 0;
  }
}
