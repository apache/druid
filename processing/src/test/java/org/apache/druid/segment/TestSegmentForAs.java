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

import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

@SuppressWarnings("rawtypes")
public class TestSegmentForAs implements Segment
{
  private final SegmentId id;
  private final Function<Class, Object> asFn;

  public TestSegmentForAs(
      SegmentId id,
      Function<Class, Object> asFn
  )
  {
    this.id = id;
    this.asFn = asFn;
  }

  @Override
  public SegmentId getId()
  {
    return id;
  }

  @Override
  public Interval getDataInterval()
  {
    return id.getInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return as(QueryableIndex.class);
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return as(StorageAdapter.class);
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    return (T) asFn.apply(clazz);
  }

  @Override
  public void close()
  {

  }
}
