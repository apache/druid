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

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.server.initialization.jetty.JettyServerModule;

import java.util.List;

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

  @JsonProperty
  private String keyManagerFactoryAlgorithm;

  @JsonProperty
  private List<String> includeCipherSuites;

  @JsonProperty
  private List<String> excludeCipherSuites;

  @JsonProperty
  private List<String> includeProtocols;

  @JsonProperty
  private List<String> excludeProtocols;

  @JsonProperty
  private boolean requireClientCertificate = false;

  @JsonProperty
  private boolean requestClientCertificate = false;

  @JsonProperty
  private String trustStoreType;

  @JsonProperty
  private String trustStorePath;

  @JsonProperty
  private String trustStoreAlgorithm;

  @JsonProperty("trustStorePassword")
  private PasswordProvider trustStorePasswordProvider;

  @JsonProperty
  private boolean validateHostnames = true;

  @JsonProperty
  private String crlPath;

  @JsonProperty
  private boolean reloadSslContext = false;

  @JsonProperty
  private int reloadSslContextSeconds = 60;

  @JsonProperty
  private boolean forceApplyConfig = false;

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

  public List<String> getIncludeCipherSuites()
  {
    return includeCipherSuites;
  }

  public List<String> getExcludeCipherSuites()
  {
    return excludeCipherSuites;
  }

  public List<String> getIncludeProtocols()
  {
    return includeProtocols;
  }

  public List<String> getExcludeProtocols()
  {
    return excludeProtocols;
  }

  public boolean isRequireClientCertificate()
  {
    return requireClientCertificate;
  }

  public boolean isRequestClientCertificate()
  {
    return requestClientCertificate;
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

  public boolean isValidateHostnames()
  {
    return validateHostnames;
  }

  public String getCrlPath()
  {
    return crlPath;
  }

  public int getReloadSslContextSeconds()
  {
    return reloadSslContextSeconds;
  }

  public boolean isReloadSslContext()
  {
    return reloadSslContext;
  }

  /**
   * Whether to apply TLS server configs even if an existing {@code SslContextFactory.Server} instance is bound.
   * See {@link JettyServerModule#makeAndInitializeServer}.
   */
  public boolean isForceApplyConfig()
  {
    return forceApplyConfig;
  }

  @Override
  public String toString()
  {
    return "TLSServerConfig{" +
           "keyStorePath='" + keyStorePath + '\'' +
           ", keyStoreType='" + keyStoreType + '\'' +
           ", certAlias='" + certAlias + '\'' +
           ", keyManagerFactoryAlgorithm='" + keyManagerFactoryAlgorithm + '\'' +
           ", includeCipherSuites=" + includeCipherSuites +
           ", excludeCipherSuites=" + excludeCipherSuites +
           ", includeProtocols=" + includeProtocols +
           ", excludeProtocols=" + excludeProtocols +
           ", requireClientCertificate=" + requireClientCertificate +
           ", requestClientCertificate=" + requestClientCertificate +
           ", trustStoreType='" + trustStoreType + '\'' +
           ", trustStorePath='" + trustStorePath + '\'' +
           ", trustStoreAlgorithm='" + trustStoreAlgorithm + '\'' +
           ", validateHostnames='" + validateHostnames + '\'' +
           ", crlPath='" + crlPath + '\'' +
           ", reloadSslContext='" + reloadSslContext + '\'' +
           ", reloadSslContextSeconds='" + reloadSslContextSeconds + '\'' +
           ", forceApplyConfig=" + forceApplyConfig +
           '}';
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private String keyStorePath;
    private String keyStoreType;
    private String certAlias;
    private PasswordProvider keyStorePasswordProvider;
    private PasswordProvider keyManagerPasswordProvider;
    private String keyManagerFactoryAlgorithm;
    private List<String> includeCipherSuites;
    private List<String> excludeCipherSuites;
    private List<String> includeProtocols;
    private List<String> excludeProtocols;
    private boolean requireClientCertificate = false;
    private boolean requestClientCertificate = false;
    private String trustStoreType;
    private String trustStorePath;
    private String trustStoreAlgorithm;
    private PasswordProvider trustStorePasswordProvider;
    private boolean validateHostnames = true;
    private String crlPath;
    private boolean reloadSslContext = false;
    private int reloadSslContextSeconds = 60;
    private boolean forceApplyConfig = false;

    public Builder keyStorePath(String keyStorePath)
    {
      this.keyStorePath = keyStorePath;
      return this;
    }

    public Builder keyStoreType(String keyStoreType)
    {
      this.keyStoreType = keyStoreType;
      return this;
    }

    public Builder certAlias(String certAlias)
    {
      this.certAlias = certAlias;
      return this;
    }

    public Builder keyStorePasswordProvider(PasswordProvider keyStorePasswordProvider)
    {
      this.keyStorePasswordProvider = keyStorePasswordProvider;
      return this;
    }

    public Builder keyManagerPasswordProvider(PasswordProvider keyManagerPasswordProvider)
    {
      this.keyManagerPasswordProvider = keyManagerPasswordProvider;
      return this;
    }

    public Builder keyManagerFactoryAlgorithm(String keyManagerFactoryAlgorithm)
    {
      this.keyManagerFactoryAlgorithm = keyManagerFactoryAlgorithm;
      return this;
    }

    public Builder includeCipherSuites(List<String> includeCipherSuites)
    {
      this.includeCipherSuites = includeCipherSuites;
      return this;
    }

    public Builder excludeCipherSuites(List<String> excludeCipherSuites)
    {
      this.excludeCipherSuites = excludeCipherSuites;
      return this;
    }

    public Builder includeProtocols(List<String> includeProtocols)
    {
      this.includeProtocols = includeProtocols;
      return this;
    }

    public Builder excludeProtocols(List<String> excludeProtocols)
    {
      this.excludeProtocols = excludeProtocols;
      return this;
    }

    public Builder requireClientCertificate(boolean requireClientCertificate)
    {
      this.requireClientCertificate = requireClientCertificate;
      return this;
    }

    public Builder requestClientCertificate(boolean requestClientCertificate)
    {
      this.requestClientCertificate = requestClientCertificate;
      return this;
    }

    public Builder trustStoreType(String trustStoreType)
    {
      this.trustStoreType = trustStoreType;
      return this;
    }

    public Builder trustStorePath(String trustStorePath)
    {
      this.trustStorePath = trustStorePath;
      return this;
    }

    public Builder trustStoreAlgorithm(String trustStoreAlgorithm)
    {
      this.trustStoreAlgorithm = trustStoreAlgorithm;
      return this;
    }

    public Builder trustStorePasswordProvider(PasswordProvider trustStorePasswordProvider)
    {
      this.trustStorePasswordProvider = trustStorePasswordProvider;
      return this;
    }

    public Builder validateHostnames(boolean validateHostnames)
    {
      this.validateHostnames = validateHostnames;
      return this;
    }

    public Builder crlPath(String crlPath)
    {
      this.crlPath = crlPath;
      return this;
    }

    public Builder reloadSslContext(boolean reloadSslContext)
    {
      this.reloadSslContext = reloadSslContext;
      return this;
    }

    public Builder reloadSslContextSeconds(int reloadSslContextSeconds)
    {
      this.reloadSslContextSeconds = reloadSslContextSeconds;
      return this;
    }

    public Builder forceApplyConfig(boolean forceApplyConfig)
    {
      this.forceApplyConfig = forceApplyConfig;
      return this;
    }

    public TLSServerConfig build()
    {
      TLSServerConfig config = new TLSServerConfig();
      config.keyStorePath = this.keyStorePath;
      config.keyStoreType = this.keyStoreType;
      config.certAlias = this.certAlias;
      config.keyStorePasswordProvider = this.keyStorePasswordProvider;
      config.keyManagerPasswordProvider = this.keyManagerPasswordProvider;
      config.keyManagerFactoryAlgorithm = this.keyManagerFactoryAlgorithm;
      config.includeCipherSuites = this.includeCipherSuites;
      config.excludeCipherSuites = this.excludeCipherSuites;
      config.includeProtocols = this.includeProtocols;
      config.excludeProtocols = this.excludeProtocols;
      config.requireClientCertificate = this.requireClientCertificate;
      config.requestClientCertificate = this.requestClientCertificate;
      config.trustStoreType = this.trustStoreType;
      config.trustStorePath = this.trustStorePath;
      config.trustStoreAlgorithm = this.trustStoreAlgorithm;
      config.trustStorePasswordProvider = this.trustStorePasswordProvider;
      config.validateHostnames = this.validateHostnames;
      config.crlPath = this.crlPath;
      config.reloadSslContext = this.reloadSslContext;
      config.reloadSslContextSeconds = this.reloadSslContextSeconds;
      config.forceApplyConfig = this.forceApplyConfig;
      return config;
    }
  }
}
