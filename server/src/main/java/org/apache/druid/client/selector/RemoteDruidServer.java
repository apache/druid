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

import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;

/**
 */
public class RemoteDruidServer implements QueryableDruidServer<DirectDruidClient>
{
  private final DruidServer server;
  private final DirectDruidClient client;

  public RemoteDruidServer(DruidServer server, DirectDruidClient client)
  {
    this.server = server;
    this.client = client;
  }

  @Override
  public DruidServer getServer()
  {
    return server;
  }

  @Override
  public DirectDruidClient getQueryRunner()
  {
    return client;
  }

  @Override
  public String toString()
  {
    return "RemoteDruidServer{" +
           "server=" + server +
           ", client=" + client +
           '}';
  }
}
