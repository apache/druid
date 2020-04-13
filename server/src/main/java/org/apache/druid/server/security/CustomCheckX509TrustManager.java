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

import org.apache.druid.java.util.common.logger.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class CustomCheckX509TrustManager extends X509ExtendedTrustManager implements X509TrustManager
{
  private static final Logger log = new Logger(CustomCheckX509TrustManager.class);

  private final X509ExtendedTrustManager delegate;
  private final boolean validateServerHostnames;
  private final TLSCertificateChecker certificateChecker;

  public CustomCheckX509TrustManager(
      final X509ExtendedTrustManager delegate,
      final TLSCertificateChecker certificateChecker,
      final boolean validateServerHostnames
  )
  {
    this.delegate = delegate;
    this.validateServerHostnames = validateServerHostnames;
    this.certificateChecker = certificateChecker;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException
  {
    delegate.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException
  {
    delegate.checkServerTrusted(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers()
  {
    return delegate.getAcceptedIssuers();
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException
  {
    delegate.checkClientTrusted(chain, authType, socket);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException
  {
    delegate.checkServerTrusted(chain, authType, socket);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException
  {
    certificateChecker.checkClient(chain, authType, engine, delegate);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException
  {
    // The Netty client we use for the internal client does not provide an option to disable the standard hostname
    // validation. When using custom certificate checks, we want to allow that option, so we change the endpoint
    // identification algorithm here. This is not needed for the server-side, since the Jetty server does provide
    // an option for enabling/disabling standard hostname validation.
    if (!validateServerHostnames) {
      SSLParameters params = engine.getSSLParameters();
      params.setEndpointIdentificationAlgorithm(null);
      engine.setSSLParameters(params);
    }

    certificateChecker.checkServer(chain, authType, engine, delegate);
  }
}
