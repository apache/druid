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

package io.druid.metadata.storage.postgresql;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.metadata.PasswordProvider;

public class PostgreSQLConnectorConfig
{
  @JsonProperty
  private boolean useSSL = false;

  @JsonProperty("sslPassword")
  private PasswordProvider sslPasswordProvider;

  @JsonProperty
  private String sslFactory;

  @JsonProperty
  private String sslFactoryArg;

  @JsonProperty
  private String sslMode;

  @JsonProperty
  private String sslCert;

  @JsonProperty
  private String sslKey;

  @JsonProperty
  private String sslRootCert;

  @JsonProperty
  private String sslHostNameVerifier;

  @JsonProperty
  private String sslPasswordCallback;


  public boolean isUseSSL()
  {
    return useSSL;
  }

  public String getPassword()
  {
    return sslPasswordProvider == null ? null : sslPasswordProvider.getPassword();
  }

  public String getSslFactory()
  {
    return sslFactory;
  }

  public String getSslFactoryArg()
  {
    return sslFactoryArg;
  }

  public String getSslMode()
  {
    return sslMode;
  }

  public String getSslCert()
  {
    return sslCert;
  }

  public String getSslKey()
  {
    return sslKey;
  }

  public String getSslRootCert()
  {
    return sslRootCert;
  }

  public String getSslHostNameVerifier()
  {
    return sslHostNameVerifier;
  }

  public String getSslPasswordCallback()
  {
    return sslPasswordCallback;
  }

  @Override
  public String toString()
  {
    return "PostgreSQLConnectorConfig{" +
           "useSSL='" + useSSL + '\'' +
           ", sslFactory='" + sslFactory + '\'' +
           ", sslFactoryArg='" + sslFactoryArg + '\'' +
           ", sslMode='" + sslMode + '\'' +
           ", sslCert='" + sslCert + '\'' +
           ", sslKey='" + sslKey + '\'' +
           ", sslRootCert='" + sslRootCert + '\'' +
           ", sslHostNameVerifier='" + sslHostNameVerifier + '\'' +
           ", sslPasswordCallback='" + sslPasswordCallback + '\'' +
           '}';
  }
}
