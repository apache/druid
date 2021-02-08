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
import com.google.common.collect.Ordering;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class ConnectionCountServerSelectorStrategy implements ServerSelectorStrategy
{
  private static final Comparator<QueryableDruidServer> COMPARATOR =
      Comparator.comparingInt(s -> ((DirectDruidClient) s.getQueryRunner()).getNumOpenConnections());

  @Nullable
  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
    return Collections.min(servers, COMPARATOR);
  }

  @Override
  public List<QueryableDruidServer> pick(Set<QueryableDruidServer> servers, DataSegment segment, int numServersToPick)
  {
    if (servers.size() <= numServersToPick) {
      return ImmutableList.copyOf(servers);
    }
    return Ordering.from(COMPARATOR).leastOf(servers, numServersToPick);
  }
}
