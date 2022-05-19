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

package org.apache.druid.security.pac4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.PasswordProvider;

public class OIDCConfig
{
  @JsonProperty
  private final String clientID;

  @JsonProperty
  private final PasswordProvider clientSecret;

  @JsonProperty
  private final String discoveryURI;

  @JsonCreator
  public OIDCConfig(
      @JsonProperty("clientID") String clientID,
      @JsonProperty("clientSecret") PasswordProvider clientSecret,
      @JsonProperty("discoveryURI") String discoveryURI
  )
  {
    this.clientID = Preconditions.checkNotNull(clientID, "null clientID");
    this.clientSecret = Preconditions.checkNotNull(clientSecret, "null clientSecret");
    this.discoveryURI = Preconditions.checkNotNull(discoveryURI, "null discoveryURI");
  }

  @JsonProperty
  public String getClientID()
  {
    return clientID;
  }

  @JsonProperty
  public PasswordProvider getClientSecret()
  {
    return clientSecret;
  }

  @JsonProperty
  public String getDiscoveryURI()
  {
    return discoveryURI;
  }
}
