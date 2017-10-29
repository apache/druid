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

package io.druid.https;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.metadata.PasswordProvider;

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

  @Override
  public String toString()
  {
    return "SSLClientConfig{" +
           "protocol='" + protocol + '\'' +
           ", trustStoreType='" + trustStoreType + '\'' +
           ", trustStorePath='" + trustStorePath + '\'' +
           ", trustStoreAlgorithm='" + trustStoreAlgorithm + '\'' +
           '}';
  }
}
