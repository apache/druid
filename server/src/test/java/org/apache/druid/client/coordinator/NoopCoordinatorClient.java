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

package org.apache.druid.client.coordinator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

public class NoopCoordinatorClient implements CoordinatorClient
{
  @Override
  public ListenableFuture<Boolean> isHandoffComplete(String dataSource, SegmentDescriptor descriptor)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<DataSegment> fetchSegment(String dataSource, String segmentId, boolean includeUnused)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<ImmutableSegmentLoadInfo> fetchServerViewSegments(String dataSource, List<Interval> intervals)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<List<DataSegment>> fetchUsedSegments(String dataSource, List<Interval> intervals)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<List<DataSourceInformation>> fetchDataSourceInformation(Set<String> datasources)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public CoordinatorClient withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    // Ignore retryPolicy for the test client.
    return this;
  }
}
