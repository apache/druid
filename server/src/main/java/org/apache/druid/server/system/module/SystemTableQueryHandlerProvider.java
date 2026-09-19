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

package org.apache.druid.server.system.module;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.system.handler.SystemTableBrokerQueryHandler;
import org.apache.druid.server.system.handler.SystemTableQueryHandler;

import java.util.Set;

/** Selects the system-table query handler for the current server role. */
public class SystemTableQueryHandlerProvider implements Provider<DataSourceQueryHandler>
{
  private final Set<NodeRole> nodeRoles;
  private final Provider<SystemTableBrokerQueryHandler> brokerQueryHandler;
  private final Provider<SystemTableQueryHandler> localQueryHandler;

  @Inject
  public SystemTableQueryHandlerProvider(
      @Self final Set<NodeRole> nodeRoles,
      final Provider<SystemTableBrokerQueryHandler> brokerQueryHandler,
      final Provider<SystemTableQueryHandler> localQueryHandler
  )
  {
    this.nodeRoles = nodeRoles;
    this.brokerQueryHandler = brokerQueryHandler;
    this.localQueryHandler = localQueryHandler;
  }

  @Override
  public DataSourceQueryHandler get()
  {
    return nodeRoles.contains(NodeRole.BROKER) ? brokerQueryHandler.get() : localQueryHandler.get();
  }
}
