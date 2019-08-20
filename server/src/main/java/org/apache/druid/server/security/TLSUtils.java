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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;
import org.eclipse.jetty.util.ssl.AliasedX509ExtendedKeyManager;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class TLSUtils
{
  private static final Logger log = new Logger(TLSUtils.class);

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
    private Boolean validateHostnames;
    private TLSCertificateChecker certificateChecker;

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

    public ClientSSLContextBuilder setValidateHostnames(Boolean validateHostnames)
    {
      this.validateHostnames = validateHostnames;
      return this;
    }

    public ClientSSLContextBuilder setCertificateChecker(TLSCertificateChecker certificateChecker)
    {
      this.certificateChecker = certificateChecker;
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
        keyManagerFactoryPasswordProvider,
        validateHostnames,
        certificateChecker
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
      @Nullable PasswordProvider keyManagerFactoryPasswordProvider,
      @Nullable Boolean validateHostnames,
      TLSCertificateChecker tlsCertificateChecker
  )
  {
    SSLContext sslContext;
    try {
      sslContext = SSLContext.getInstance(protocol == null ? "TLSv1.2" : protocol);
      KeyStore trustStore = KeyStore.getInstance(trustStoreType == null
                                               ? KeyStore.getDefaultType()
                                               : trustStoreType);
      try (final InputStream trustStoreFileStream = Files.newInputStream(Paths.get(trustStorePath))) {
        trustStore.load(
            trustStoreFileStream,
            trustStorePasswordProvider == null ? null : trustStorePasswordProvider.getPassword().toCharArray()
        );
      }
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustStoreAlgorithm == null
                                                                                ? TrustManagerFactory.getDefaultAlgorithm()
                                                                                : trustStoreAlgorithm);
      trustManagerFactory.init(trustStore);

      KeyManager[] keyManagers;
      if (keyStorePath != null) {
        KeyStore keyStore = KeyStore.getInstance(keyStoreType == null
                                                 ? KeyStore.getDefaultType()
                                                 : keyStoreType);
        try (final InputStream keyStoreFileStream = Files.newInputStream(Paths.get(keyStorePath))) {
          keyStore.load(
              keyStoreFileStream,
              keyStorePasswordProvider == null ? null : keyStorePasswordProvider.getPassword().toCharArray()
          );

          KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
              keyStoreAlgorithm == null ?
              KeyManagerFactory.getDefaultAlgorithm() : keyStoreAlgorithm
          );
          keyManagerFactory.init(
              keyStore,
              keyManagerFactoryPasswordProvider == null
              ? null
              : keyManagerFactoryPasswordProvider.getPassword().toCharArray()
          );
          keyManagers = createAliasedKeyManagers(keyManagerFactory.getKeyManagers(), certAlias);
        }
      } else {
        keyManagers = null;
      }

      TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
      TrustManager[] newTrustManagers = new TrustManager[trustManagers.length];

      for (int i = 0; i < trustManagers.length; i++) {
        if (trustManagers[i] instanceof X509ExtendedTrustManager) {
          newTrustManagers[i] = new CustomCheckX509TrustManager(
              (X509ExtendedTrustManager) trustManagers[i],
              tlsCertificateChecker,
              validateHostnames == null ? true : validateHostnames
          );
        } else {
          newTrustManagers[i] = trustManagers[i];
          log.info("Encountered non-X509ExtendedTrustManager: " + trustManagers[i].getClass());
        }
      }

      sslContext.init(
          keyManagers,
          newTrustManagers,
          null
      );
    }
    catch (CertificateException | KeyManagementException | IOException | KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
      throw new RuntimeException(e);
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
