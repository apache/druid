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

package org.apache.druid.server.router;

import org.apache.druid.client.selector.Server;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConsistentHashAvaticaConnectionBalancer implements AvaticaConnectionBalancer
{
  private final ConsistentHasher hasher = new ConsistentHasher(null);

  @Override
  public Server pickServer(Collection<Server> servers, String connectionId)
  {
    synchronized (hasher) {
      if (servers.isEmpty()) {
        return null;
      }
      Map<String, Server> serverMap = new HashMap<>();
      for (Server server : servers) {
        serverMap.put(server.getHost(), server);
      }

      hasher.updateKeys(serverMap.keySet());
      String chosenServer = hasher.findKey(StringUtils.toUtf8(connectionId));
      return serverMap.get(chosenServer);
    }
  }
}
