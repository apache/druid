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

/**
 * This class exists only to support two REST endpoints. It is similar to a
 * DruidNode, but serializes to the specific form expected by those REST
 * endpoints. This version omits {@code bindOnHost}, uses the name
 * {@code `service} where DruidNode uses {@code serviceName}, and
 * omits the TLS or Plaintext port rather than using -1 and flags
 * as in DruidNode. Think of this as a purpose-build facade onto a
 * Druid Node.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Node
{
  private final DruidNode node;

  @JsonCreator
  public Node(DruidNode node)
  {
    this.node = node;
  }

  @JsonProperty
  public String getHost()
  {
    return node.getHost();
  }

  @JsonProperty
  public String getService()
  {
    return node.getServiceName();
  }

  @JsonProperty
  public Integer getPlaintextPort()
  {
    return node.isEnablePlaintextPort() ? node.getPlaintextPort() : null;
  }

  @JsonProperty
  public Integer getTlsPort()
  {
    return node.isEnableTlsPort() ? node.getTlsPort() : null;
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
    return Objects.equal(this.getHost(), other.getHost()) &&
           Objects.equal(this.getService(), other.getService()) &&
           Objects.equal(this.getPlaintextPort(), other.getPlaintextPort()) &&
           Objects.equal(this.getTlsPort(), other.getTlsPort());
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(getHost(), getService(), getPlaintextPort(), getTlsPort());
  }
}
