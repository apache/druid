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

package org.apache.druid.server.scheduling;

import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.QueryLaningStrategy;

import java.util.Set;

public class NoQueryLaningStrategy implements QueryLaningStrategy
{
  private static final Object2IntMap<String> NONE = new Object2IntArrayMap<>();

  public static NoQueryLaningStrategy INSTANCE = new NoQueryLaningStrategy();

  @Override
  public Object2IntMap<String> getLaneLimits()
  {
    return NONE;
  }

  @Override
  public <T> Query<T> laneQuery(QueryPlus<T> query, Set<SegmentServerSelector> segments)
  {
    return query.getQuery();
  }
}
