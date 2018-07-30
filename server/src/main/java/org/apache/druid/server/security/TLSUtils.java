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

import com.google.common.base.Throwables;
import org.apache.druid.metadata.PasswordProvider;
import org.eclipse.jetty.util.ssl.AliasedX509ExtendedKeyManager;

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
  public static SSLContext createSSLContext(
      String protocol,
      String trustStoreType,
      String trustStorePath,
      String trustStoreAlgorithm,
      PasswordProvider trustStorePasswordProvider,
      String keyStoreType,
      String keyStorePath,
      String keyStoreAlgorithm,
      String certAlias,
      PasswordProvider keyStorePasswordProvider,
      PasswordProvider keyManagerFactoryPasswordProvider
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
          trustStorePasswordProvider.getPassword().toCharArray()
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
