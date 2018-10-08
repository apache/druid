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

package org.apache.druid.client;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.server.initialization.ZkPathsConfig;

import javax.validation.constraints.NotNull;

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
    return new SingleServerInventoryView(zkPaths, curator, jsonMapper, Predicates.alwaysFalse());
  }
}
