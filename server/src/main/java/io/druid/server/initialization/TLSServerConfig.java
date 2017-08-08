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
package io.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.metadata.PasswordProvider;

public class TLSServerConfig
{
  @JsonProperty
  private String keyStorePath;

  @JsonProperty
  private String keyStoreType;

  @JsonProperty
  private String certAlias;

  @JsonProperty("keyStorePassword")
  private PasswordProvider keyStorePasswordProvider;

  @JsonProperty("keyManagerPassword")
  private PasswordProvider keyManagerPasswordProvider;

  public String getKeyStorePath()
  {
    return keyStorePath;
  }

  public String getKeyStoreType()
  {
    return keyStoreType;
  }

  public PasswordProvider getKeyStorePasswordProvider()
  {
    return keyStorePasswordProvider;
  }

  public String getCertAlias()
  {
    return certAlias;
  }

  public PasswordProvider getKeyManagerPasswordProvider()
  {
    return keyManagerPasswordProvider;
  }

  @Override
  public String toString()
  {
    return "TLSServerConfig{" +
           "keyStorePath='" + keyStorePath + '\'' +
           ", keyStoreType='" + keyStoreType + '\'' +
           ", certAlias='" + certAlias + '\'' +
           '}';
  }
}
