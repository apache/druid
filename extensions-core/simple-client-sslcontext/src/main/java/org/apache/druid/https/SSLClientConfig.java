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

package org.apache.druid.https;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.PasswordProvider;

public class SSLClientConfig
{
  @JsonProperty
  private String protocol;

  @JsonProperty
  private String trustStoreType;

  @JsonProperty
  private String trustStorePath;

  @JsonProperty
  private String trustStoreAlgorithm;

  @JsonProperty("trustStorePassword")
  private PasswordProvider trustStorePasswordProvider;

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

  @JsonProperty
  private String keyManagerFactoryAlgorithm;

  @JsonProperty
  private Boolean validateHostnames;

  public String getProtocol()
  {
    return protocol;
  }

  public String getTrustStoreType()
  {
    return trustStoreType;
  }

  public String getTrustStorePath()
  {
    return trustStorePath;
  }

  public String getTrustStoreAlgorithm()
  {
    return trustStoreAlgorithm;
  }

  public PasswordProvider getTrustStorePasswordProvider()
  {
    return trustStorePasswordProvider;
  }

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

  public String getKeyManagerFactoryAlgorithm()
  {
    return keyManagerFactoryAlgorithm;
  }

  public Boolean getValidateHostnames()
  {
    return validateHostnames;
  }

  @Override
  public String toString()
  {
    return "SSLClientConfig{" +
           "protocol='" + protocol + '\'' +
           ", trustStoreType='" + trustStoreType + '\'' +
           ", trustStorePath='" + trustStorePath + '\'' +
           ", trustStoreAlgorithm='" + trustStoreAlgorithm + '\'' +
           ", keyStorePath='" + keyStorePath + '\'' +
           ", keyStoreType='" + keyStoreType + '\'' +
           ", certAlias='" + certAlias + '\'' +
           ", keyManagerFactoryAlgorithm='" + keyManagerFactoryAlgorithm + '\'' +
           ", validateHostnames='" + validateHostnames + '\'' +
           '}';
  }
}
