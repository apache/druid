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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Node
{
  private final String host;
  private final String service;
  private final Integer plaintextPort;
  private final Integer tlsPort;

  @JsonCreator
  public Node(String host, String service, Integer plaintextPort, Integer tlsPort)
  {
    this.host = host;
    this.service = service;
    this.plaintextPort = plaintextPort;
    this.tlsPort = tlsPort;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public String getService()
  {
    return service;
  }

  @JsonProperty
  public Integer getPlaintextPort()
  {
    return plaintextPort;
  }

  @JsonProperty
  public Integer getTlsPort()
  {
    return tlsPort;
  }

  public static Node from(DruidNode druidNode)
  {
    return new Node(
        druidNode.getHost(),
        druidNode.getServiceName(),
        druidNode.getPlaintextPort() > 0 ? druidNode.getPlaintextPort() : null,
        druidNode.getTlsPort() > 0 ? druidNode.getTlsPort() : null
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof Node)) {
      return false;
    }
    Node other = (Node) o;
    return Objects.equal(this.host, other.host) &&
           Objects.equal(this.service, other.service) &&
           Objects.equal(this.plaintextPort, other.plaintextPort) &&
           Objects.equal(this.tlsPort, other.tlsPort);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(host, service, plaintextPort, tlsPort);
  }
}
