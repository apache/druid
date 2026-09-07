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

package org.apache.druid.consul.discovery;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;

/**
 * SSL/TLS configuration specific to Consul client connections.
 * This is isolated from global Druid SSL configuration to avoid side effects.
 */
@SuppressWarnings({"unused", "deprecation"})
public class ConsulSSLConfig
{
  @JsonProperty
  @Nullable
  private String protocol;

  @JsonProperty
  @Nullable
  private String trustStoreType;

  @JsonProperty
  @Nullable
  private String trustStorePath;

  @JsonProperty
  @Nullable
  private String trustStoreAlgorithm;

  @JsonProperty("trustStorePassword")
  @Nullable
  private PasswordProvider trustStorePasswordProvider;

  @JsonProperty
  @Nullable
  private String keyStorePath;

  @JsonProperty
  @Nullable
  private String keyStoreType;

  @JsonProperty
  @Nullable
  private String certAlias;

  @JsonProperty("keyStorePassword")
  @Nullable
  private PasswordProvider keyStorePasswordProvider;

  @JsonProperty("keyManagerPassword")
  @Nullable
  private PasswordProvider keyManagerPasswordProvider;

  @JsonProperty
  @Nullable
  private String keyManagerFactoryAlgorithm;

  @JsonProperty
  @Nullable
  private Boolean validateHostnames;

  @Nullable
  public String getProtocol()
  {
    return protocol;
  }

  @Nullable
  public String getTrustStoreType()
  {
    return trustStoreType;
  }

  @Nullable
  public String getTrustStorePath()
  {
    return trustStorePath;
  }

  @Nullable
  public String getTrustStoreAlgorithm()
  {
    return trustStoreAlgorithm;
  }

  @Nullable
  public PasswordProvider getTrustStorePasswordProvider()
  {
    return trustStorePasswordProvider;
  }

  @Nullable
  public String getKeyStorePath()
  {
    return keyStorePath;
  }

  @Nullable
  public String getKeyStoreType()
  {
    return keyStoreType;
  }

  @Nullable
  public PasswordProvider getKeyStorePasswordProvider()
  {
    return keyStorePasswordProvider;
  }

  @Nullable
  public String getCertAlias()
  {
    return certAlias;
  }

  @Nullable
  public PasswordProvider getKeyManagerPasswordProvider()
  {
    return keyManagerPasswordProvider;
  }

  @Nullable
  public String getKeyManagerFactoryAlgorithm()
  {
    return keyManagerFactoryAlgorithm;
  }

  @Nullable
  public Boolean getValidateHostnames()
  {
    return validateHostnames;
  }

  @Override
  public String toString()
  {
    return "ConsulSSLConfig{" +
           "protocol='" + protocol + '\'' +
           ", trustStoreType='" + trustStoreType + '\'' +
           ", trustStorePath='" + trustStorePath + '\'' +
           ", trustStoreAlgorithm='" + trustStoreAlgorithm + '\'' +
           ", keyStorePath='" + keyStorePath + '\'' +
           ", keyStoreType='" + keyStoreType + '\'' +
           ", certAlias='" + certAlias + '\'' +
           ", keyManagerFactoryAlgorithm='" + keyManagerFactoryAlgorithm + '\'' +
           ", validateHostnames=" + validateHostnames +
           '}';
  }
}
