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

package org.apache.druid.client.selector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServerSelectorStrategy implements ServerSelectorStrategy
{
  @Nullable
  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
    return pickOne(servers);
  }

  @Override
  public List<QueryableDruidServer> pick(Set<QueryableDruidServer> servers, DataSegment segment, int numServersToPick)
  {
    if (servers.size() <= numServersToPick) {
      return ImmutableList.copyOf(servers);
    }
    if (numServersToPick == 1) {
      // Tier strategies pick a single server through this method for every segment of every query, so choose it
      // directly rather than copying and shuffling the set. Each server is equally likely either way.
      return ImmutableList.of(pickOne(servers));
    }
    List<QueryableDruidServer> list = Lists.newArrayList(servers);
    Collections.shuffle(list, ThreadLocalRandom.current());
    return ImmutableList.copyOf(list.subList(0, numServersToPick));
  }

  private static QueryableDruidServer pickOne(Set<QueryableDruidServer> servers)
  {
    return Iterators.get(servers.iterator(), ThreadLocalRandom.current().nextInt(servers.size()));
  }
}
