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

package io.druid.client;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;

import io.druid.java.util.common.Pair;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import javax.validation.constraints.NotNull;

/**
 * @deprecated legacy
 */
@Deprecated
public class FilteredSingleServerInventoryViewProvider implements FilteredServerInventoryViewProvider
{
  @JacksonInject
  @NotNull
  private ZkPathsConfig zkPaths = null;

  @JacksonInject
  @NotNull
  private CuratorFramework curator = null;

  @JacksonInject
  @NotNull
  private ObjectMapper jsonMapper = null;

  @Override
  public SingleServerInventoryView get()
  {
    return new SingleServerInventoryView(zkPaths, curator, jsonMapper, Predicates.<Pair<DruidServerMetadata, DataSegment>>alwaysFalse());
  }
}
