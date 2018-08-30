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

package org.apache.druid.metadata.storage.mysql;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.PasswordProvider;

import java.util.List;

public class MySQLConnectorConfig
{
  @JsonProperty
  private boolean useSSL = false;

  @JsonProperty
  private String trustCertificateKeyStoreUrl;

  @JsonProperty
  private String trustCertificateKeyStoreType;

  @JsonProperty("trustCertificateKeyStorePassword")
  private PasswordProvider trustCertificateKeyStorePasswordProvider;

  @JsonProperty
  private String clientCertificateKeyStoreUrl;

  @JsonProperty
  private String clientCertificateKeyStoreType;

  @JsonProperty("clientCertificateKeyStorePassword")
  private PasswordProvider clientCertificateKeyStorePasswordProvider;

  @JsonProperty
  private List<String> enabledSSLCipherSuites;

  @JsonProperty
  private List<String> enabledTLSProtocols;

  @JsonProperty
  private boolean verifyServerCertificate = false;

  public boolean isUseSSL()
  {
    return useSSL;
  }

  public String getTrustCertificateKeyStoreUrl()
  {
    return trustCertificateKeyStoreUrl;
  }

  public String getTrustCertificateKeyStoreType()
  {
    return trustCertificateKeyStoreType;
  }

  public String getTrustCertificateKeyStorePassword()
  {
    return trustCertificateKeyStorePasswordProvider == null ? null : trustCertificateKeyStorePasswordProvider.getPassword();
  }

  public String getClientCertificateKeyStoreUrl()
  {
    return clientCertificateKeyStoreUrl;
  }

  public String getClientCertificateKeyStoreType()
  {
    return clientCertificateKeyStoreType;
  }

  public String getClientCertificateKeyStorePassword()
  {
    return clientCertificateKeyStorePasswordProvider == null ? null : clientCertificateKeyStorePasswordProvider.getPassword();
  }

  public List<String> getEnabledSSLCipherSuites()
  {
    return enabledSSLCipherSuites;
  }

  public List<String> getEnabledTLSProtocols()
  {
    return enabledTLSProtocols;
  }

  public boolean isVerifyServerCertificate()
  {
    return verifyServerCertificate;
  }

  @Override
  public String toString()
  {
    return "MySQLConnectorConfig{" +
           "useSSL='" + useSSL + '\'' +
           ", clientCertificateKeyStoreUrl='" + clientCertificateKeyStoreUrl + '\'' +
           ", clientCertificateKeyStoreType='" + clientCertificateKeyStoreType + '\'' +
           ", verifyServerCertificate='" + verifyServerCertificate + '\'' +
           ", trustCertificateKeyStoreUrl='" + trustCertificateKeyStoreUrl + '\'' +
           ", trustCertificateKeyStoreType='" + trustCertificateKeyStoreType + '\'' +
           ", enabledSSLCipherSuites=" + enabledSSLCipherSuites +
           ", enabledTLSProtocols=" + enabledTLSProtocols +
           '}';
  }
}
