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

package io.druid.client.selector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServerSelectorStrategy implements ServerSelectorStrategy
{
  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
    return Iterators.get(servers.iterator(), ThreadLocalRandom.current().nextInt(servers.size()));
  }

  @Override
  public List<QueryableDruidServer> pick(Set<QueryableDruidServer> servers, DataSegment segment, int numServersToPick)
  {
    if (servers.size() <= numServersToPick) {
      return ImmutableList.copyOf(servers);
    }
    List<QueryableDruidServer> list = Lists.newArrayList(servers);
    Collections.shuffle(list, ThreadLocalRandom.current());
    return ImmutableList.copyOf(list.subList(0, numServersToPick));
  }
}
