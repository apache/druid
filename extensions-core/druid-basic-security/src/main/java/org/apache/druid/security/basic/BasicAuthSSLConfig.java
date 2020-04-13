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

package org.apache.druid.security.basic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.PasswordProvider;

public class BasicAuthSSLConfig
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

  @JsonCreator
  public BasicAuthSSLConfig(
      @JsonProperty("protocol") String protocol,
      @JsonProperty("trustStoreType") String trustStoreType,
      @JsonProperty("trustStorePath") String trustStorePath,
      @JsonProperty("trustStoreAlgorithm") String trustStoreAlgorithm,
      @JsonProperty("trustStorePassword") PasswordProvider trustStorePasswordProvider,
      @JsonProperty("keyStorePath") String keyStorePath,
      @JsonProperty("keyStoreType") String keyStoreType,
      @JsonProperty("certAlias") String certAlias,
      @JsonProperty("keyStorePassword") PasswordProvider keyStorePasswordProvider,
      @JsonProperty("keyManagerPassword") PasswordProvider keyManagerPasswordProvider,
      @JsonProperty("keyManagerFactoryAlgorithm") String keyManagerFactoryAlgorithm,
      @JsonProperty("validateHostnames") Boolean validateHostnames
  )
  {
    this.protocol = protocol;
    this.trustStoreType = trustStoreType;
    this.trustStorePath = trustStorePath;
    this.trustStoreAlgorithm = trustStoreAlgorithm;
    this.trustStorePasswordProvider = trustStorePasswordProvider;
    this.keyStorePath = keyStorePath;
    this.keyStoreType = keyStoreType;
    this.certAlias = certAlias;
    this.keyStorePasswordProvider = keyStorePasswordProvider;
    this.keyManagerPasswordProvider = keyManagerPasswordProvider;
    this.keyManagerFactoryAlgorithm = keyManagerFactoryAlgorithm;
    this.validateHostnames = validateHostnames;
  }

  @JsonProperty
  public String getProtocol()
  {
    return protocol;
  }

  @JsonProperty
  public String getTrustStoreType()
  {
    return trustStoreType;
  }

  @JsonProperty
  public String getTrustStorePath()
  {
    return trustStorePath;
  }

  @JsonProperty
  public String getTrustStoreAlgorithm()
  {
    return trustStoreAlgorithm;
  }

  @JsonProperty("trustStorePassword")
  public PasswordProvider getTrustStorePasswordProvider()
  {
    return trustStorePasswordProvider;
  }

  @JsonProperty
  public String getKeyStorePath()
  {
    return keyStorePath;
  }

  @JsonProperty
  public String getKeyStoreType()
  {
    return keyStoreType;
  }

  @JsonProperty
  public String getCertAlias()
  {
    return certAlias;
  }

  @JsonProperty("keyStorePassword")
  public PasswordProvider getKeyStorePasswordProvider()
  {
    return keyStorePasswordProvider;
  }

  @JsonProperty("keyManagerPassword")
  public PasswordProvider getKeyManagerPasswordProvider()
  {
    return keyManagerPasswordProvider;
  }

  @JsonProperty
  public String getKeyManagerFactoryAlgorithm()
  {
    return keyManagerFactoryAlgorithm;
  }

  @JsonProperty
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
