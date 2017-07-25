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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.emitter.EmittingLogger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class SSLContextProvider implements Provider<SSLContext>
{
  private static final EmittingLogger log = new EmittingLogger(SSLContextProvider.class);

  private SSLClientConfig config;

  @Inject
  public SSLContextProvider(SSLClientConfig config)
  {
    this.config = config;
  }

  @Override
  public SSLContext get()
  {
    log.info("Creating SslContext for https client using config [%s]", config);

    SSLContext sslContext = null;
    try {
      sslContext = SSLContext.getInstance(config.getProtocol() == null ? "TLSv1.2" : config.getProtocol());
      KeyStore keyStore = KeyStore.getInstance(config.getTrustStoreType() == null
                                               ? KeyStore.getDefaultType()
                                               : config.getTrustStoreType());
      keyStore.load(
          new FileInputStream(config.getTrustStorePath()),
          config.getTrustStorePasswordProvider().getPassword().toCharArray()
      );
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(config.getTrustStoreAlgorithm() == null
                                                                                ? TrustManagerFactory.getDefaultAlgorithm()
                                                                                : config.getTrustStoreAlgorithm());
      trustManagerFactory.init(keyStore);
      sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
    }
    catch (CertificateException | KeyManagementException | IOException | KeyStoreException | NoSuchAlgorithmException e) {
      Throwables.propagate(e);
    }
    return sslContext;
  }
}
