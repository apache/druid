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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.scheduling.HiLoQueryLaningStrategy;
import org.apache.druid.server.scheduling.ManualQueryLaningStrategy;
import org.apache.druid.server.scheduling.NoQueryLaningStrategy;

import java.util.Optional;
import java.util.Set;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "strategy", defaultImpl = NoQueryLaningStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "none", value = NoQueryLaningStrategy.class),
    @JsonSubTypes.Type(name = "hilo", value = HiLoQueryLaningStrategy.class),
    @JsonSubTypes.Type(name = "manual", value = ManualQueryLaningStrategy.class)
})
public interface QueryLaningStrategy
{
  /**
   * Provide a map of lane names to the limit on the number of concurrent queries for that lane
   * @param totalLimit
   */
  Object2IntMap<String> getLaneLimits(int totalLimit);

  /**
   * For a given {@link QueryPlus} and set of {@link SegmentServerSelector}, compute if a query belongs to a lane
   *
   * This method must be thread safe
   */
  <T> Optional<String> computeLane(QueryPlus<T> query, Set<SegmentServerSelector> segments);

  default int computeLimitFromPercent(int totalLimit, int value)
  {
    return Ints.checkedCast((long) Math.ceil(totalLimit * ((double) value / 100)));
  }
}
