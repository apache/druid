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

package org.apache.druid.server.emitter;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.PasswordProvider;

/**
 * This is kept separate from {@link org.apache.druid.java.util.emitter.core.HttpEmitterConfig} because {@link PasswordProvider}
 * is currently located in druid-api. The java-util module which contains HttpEmitterConfig cannot import
 * PasswordProvider because this would introduce a circular dependence between java-util and druid-api.
 *
 * PasswordProvider could be moved to java-util, but PasswordProvider is annotated with
 * {@link org.apache.druid.guice.annotations.ExtensionPoint}, which would also have to be moved.
 *
 * It would be easier to resolve these issues and merge the TLS-related config with HttpEmitterConfig once
 * https://github.com/apache/incubator-druid/issues/4312 is resolved, so the TLS config is kept separate for now.
 */
public class HttpEmitterSSLClientConfig
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

  @JsonProperty("useDefaultJavaContext")
  private boolean useDefaultJavaContext = false;

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

  public boolean isUseDefaultJavaContext()
  {
    return useDefaultJavaContext;
  }

  @Override
  public String toString()
  {
    return "HttpEmitterSSLClientConfig{" +
           "protocol='" + protocol + '\'' +
           ", trustStoreType='" + trustStoreType + '\'' +
           ", trustStorePath='" + trustStorePath + '\'' +
           ", trustStoreAlgorithm='" + trustStoreAlgorithm + '\'' +
           ", useDefaultJavaContext='" + useDefaultJavaContext + '\'' +
           '}';
  }
}
