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

package org.apache.druid.server.security;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.druid.metadata.PasswordProvider;

import org.eclipse.jetty.util.ssl.AliasedX509ExtendedKeyManager;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class TLSUtils
{
  public static class ClientSSLContextBuilder
  {
    private String protocol;
    private String trustStoreType;
    private String trustStorePath;
    private String trustStoreAlgorithm;
    private PasswordProvider trustStorePasswordProvider;
    private String keyStoreType;
    private String keyStorePath;
    private String keyStoreAlgorithm;
    private String certAlias;
    private PasswordProvider keyStorePasswordProvider;
    private PasswordProvider keyManagerFactoryPasswordProvider;

    public ClientSSLContextBuilder setProtocol(String protocol)
    {
      this.protocol = protocol;
      return this;
    }

    public ClientSSLContextBuilder setTrustStoreType(String trustStoreType)
    {
      this.trustStoreType = trustStoreType;
      return this;
    }

    public ClientSSLContextBuilder setTrustStorePath(String trustStorePath)
    {
      this.trustStorePath = trustStorePath;
      return this;
    }

    public ClientSSLContextBuilder setTrustStoreAlgorithm(String trustStoreAlgorithm)
    {
      this.trustStoreAlgorithm = trustStoreAlgorithm;
      return this;
    }

    public ClientSSLContextBuilder setTrustStorePasswordProvider(PasswordProvider trustStorePasswordProvider)
    {
      this.trustStorePasswordProvider = trustStorePasswordProvider;
      return this;
    }

    public ClientSSLContextBuilder setKeyStoreType(String keyStoreType)
    {
      this.keyStoreType = keyStoreType;
      return this;
    }

    public ClientSSLContextBuilder setKeyStorePath(String keyStorePath)
    {
      this.keyStorePath = keyStorePath;
      return this;
    }

    public ClientSSLContextBuilder setKeyStoreAlgorithm(String keyStoreAlgorithm)
    {
      this.keyStoreAlgorithm = keyStoreAlgorithm;
      return this;
    }

    public ClientSSLContextBuilder setCertAlias(String certAlias)
    {
      this.certAlias = certAlias;
      return this;
    }

    public ClientSSLContextBuilder setKeyStorePasswordProvider(PasswordProvider keyStorePasswordProvider)
    {
      this.keyStorePasswordProvider = keyStorePasswordProvider;
      return this;
    }

    public ClientSSLContextBuilder setKeyManagerFactoryPasswordProvider(PasswordProvider keyManagerFactoryPasswordProvider)
    {
      this.keyManagerFactoryPasswordProvider = keyManagerFactoryPasswordProvider;
      return this;
    }

    public SSLContext build()
    {
      Preconditions.checkNotNull(trustStorePath, "must specify a trustStorePath");

      return createSSLContext(
        protocol,
        trustStoreType,
        trustStorePath,
        trustStoreAlgorithm,
        trustStorePasswordProvider,
        keyStoreType,
        keyStorePath,
        keyStoreAlgorithm,
        certAlias,
        keyStorePasswordProvider,
        keyManagerFactoryPasswordProvider
      );
    }
  }

  public static SSLContext createSSLContext(
      @Nullable String protocol,
      @Nullable String trustStoreType,
      String trustStorePath,
      @Nullable String trustStoreAlgorithm,
      @Nullable PasswordProvider trustStorePasswordProvider,
      @Nullable String keyStoreType,
      @Nullable String keyStorePath,
      @Nullable String keyStoreAlgorithm,
      @Nullable String certAlias,
      @Nullable PasswordProvider keyStorePasswordProvider,
      @Nullable PasswordProvider keyManagerFactoryPasswordProvider
  )
  {
    SSLContext sslContext = null;
    try {
      sslContext = SSLContext.getInstance(protocol == null ? "TLSv1.2" : protocol);
      KeyStore trustStore = KeyStore.getInstance(trustStoreType == null
                                               ? KeyStore.getDefaultType()
                                               : trustStoreType);
      trustStore.load(
          new FileInputStream(trustStorePath),
          trustStorePasswordProvider == null ? null : trustStorePasswordProvider.getPassword().toCharArray()
      );
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm == null
                                                                                ? TrustManagerFactory.getDefaultAlgorithm()
                                                                                : trustStoreAlgorithm);
      trustManagerFactory.init(trustStore);


      KeyManager[] keyManagers;
      if (keyStorePath != null) {
        KeyStore keyStore = KeyStore.getInstance(keyStoreType == null
                                                 ? KeyStore.getDefaultType()
                                                 : keyStoreType);
        keyStore.load(
            new FileInputStream(keyStorePath),
            keyStorePasswordProvider == null ? null : keyStorePasswordProvider.getPassword().toCharArray()
        );

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
            keyStoreAlgorithm == null ?
            KeyManagerFactory.getDefaultAlgorithm() : keyStoreAlgorithm
        );
        keyManagerFactory.init(
            keyStore,
            keyManagerFactoryPasswordProvider == null ? null : keyManagerFactoryPasswordProvider.getPassword().toCharArray()
        );
        keyManagers = createAliasedKeyManagers(keyManagerFactory.getKeyManagers(), certAlias);
      } else {
        keyManagers = null;
      }

      sslContext.init(
          keyManagers,
          trustManagerFactory.getTrustManagers(),
          null
      );
    }
    catch (CertificateException | KeyManagementException | IOException | KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
      Throwables.propagate(e);
    }
    return sslContext;
  }

  // Use Jetty's aliased KeyManager for consistency between server/client TLS configs
  private static KeyManager[] createAliasedKeyManagers(KeyManager[] delegates, String certAlias)
  {
    KeyManager[] aliasedManagers = new KeyManager[delegates.length];
    for (int i = 0; i < delegates.length; i++) {
      if (delegates[i] instanceof X509ExtendedKeyManager) {
        aliasedManagers[i] = new AliasedX509ExtendedKeyManager((X509ExtendedKeyManager) delegates[i], certAlias);
      } else {
        aliasedManagers[i] = delegates[i];
      }
    }
    return aliasedManagers;
  }
}
