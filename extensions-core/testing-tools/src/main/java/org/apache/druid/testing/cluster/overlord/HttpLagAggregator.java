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

package org.apache.druid.testing.cluster.overlord;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.seekablestream.supervisor.LagAggregator;

import java.util.Map;

/**
 * {@link LagAggregator} that returns lag values tracked by the Overlord in-memory.
 * The lag values are maintained in {@link SupervisorLagTracker} and may be updated
 * by calling test-only Overlord APIs.
 */
public class HttpLagAggregator implements LagAggregator
{
  private final SupervisorLagTracker lagTracker;
  private final LagAggregator delegate = LagAggregator.DEFAULT;

  @JsonCreator
  public HttpLagAggregator(
      @JacksonInject SupervisorLagTracker lagTracker
  )
  {
    this.lagTracker = lagTracker;
  }

  @Override
  public <PartitionIdType> LagStats aggregate(String supervisorId, Map<PartitionIdType, Long> partitionLags)
  {
    final LagStats tracked = lagTracker.getLag(supervisorId);
    if (tracked == null) {
      return delegate.aggregate(supervisorId, partitionLags);
    } else {
      return tracked;
    }
  }
}
