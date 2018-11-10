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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.client.selector.Server;

import java.util.Collection;

/**
 * An AvaticaConnectionBalancer balances Avatica connections across a collection of servers.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = RendezvousHashAvaticaConnectionBalancer.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "rendezvousHash", value = RendezvousHashAvaticaConnectionBalancer.class),
    @JsonSubTypes.Type(name = "consistentHash", value = ConsistentHashAvaticaConnectionBalancer.class)
})
public interface AvaticaConnectionBalancer
{
  /**
   * @param servers Servers to balance across
   * @param connectionId Connection ID to be balanced
   * @return Server that connectionId should be assigned to. The process for choosing a server must be deterministic and
   *         sticky (with a fixed set of servers, the same connectionId should always be assigned to the same server)
   */
  Server pickServer(Collection<Server> servers, String connectionId);
}
