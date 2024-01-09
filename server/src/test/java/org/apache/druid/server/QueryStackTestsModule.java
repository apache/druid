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

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import java.util.HashMap;
import java.util.Map;

public class QueryStackTestsModule extends AbstractModule
{
  public static final Key<Map> TIMELINES_KEY = Key.get(Map.class, Names.named("timelines"));


  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> timelines = new HashMap<>();

  @Provides
  @Named("timelines")
  public Map getTimelines()
  {
    return timelines;
  }

  @Override
  protected void configure()
  {
  }
}
