/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.curator.announcement.Announcer;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.config.TierForkZkConfig;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.DruidNode;

import java.io.IOException;

// Announces that it has a task
public class ForkAnnouncer
{
  private final Announcer announcer;
  private final DruidNode druidNode;
  private final ObjectMapper jsonMapper;
  private final String path;

  @Inject
  public ForkAnnouncer(
      Announcer announcer,
      @Self DruidNode self,
      @Json ObjectMapper mapper,
      TierForkZkConfig tierForkZkConfig,
      Task task
  )
  {
    this.announcer = announcer;
    druidNode = self;
    jsonMapper = mapper;
    path = tierForkZkConfig.getTierTaskIDPath(task.getId());
  }

  @LifecycleStart
  public void announceTask()
  {
    try {
      announcer.announce(path, jsonMapper.writeValueAsBytes(druidNode));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @LifecycleStop
  public void unannounceTask()
  {
    announcer.unannounce(path);
  }
}
