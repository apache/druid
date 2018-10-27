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

import org.apache.druid.guice.annotations.ExtensionPoint;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * This extension point allows developers to replace the standard TLS certificate checks with custom checks.
 * By default, a {@link DefaultTLSCertificateChecker} is used, which simply delegates to the
 * base {@link X509ExtendedTrustManager}.
 */
@ExtensionPoint
public interface TLSCertificateChecker
{
  /**
   * This method allows an extension to replace the standard
   * {@link X509ExtendedTrustManager#checkClientTrusted(X509Certificate[], String, SSLEngine)} method.
   *
   * This controls the certificate check used by Druid's server, checking certificates for internal requests made
   * by other Druid services and user-submitted requests.
   *
   * @param chain See docs for {@link X509ExtendedTrustManager#checkClientTrusted(X509Certificate[], String, SSLEngine)}.
   * @param authType See docs for {@link X509ExtendedTrustManager#checkClientTrusted(X509Certificate[], String, SSLEngine)}.
   * @param engine See docs for {@link X509ExtendedTrustManager#checkClientTrusted(X509Certificate[], String, SSLEngine)}.
   * @param baseTrustManager The base trust manager. An extension should call
   *                         baseTrustManager.checkClientTrusted(chain, authType, engine) if/when it wishes
   *                         to use the standard check in addition to custom checks.
   * @throws CertificateException
   */
  void checkClient(
      X509Certificate[] chain,
      String authType,
      SSLEngine engine,
      X509ExtendedTrustManager baseTrustManager
  ) throws CertificateException;

  /**
   * This method allows an extension to replace the standard
   * {@link X509ExtendedTrustManager#checkServerTrusted(X509Certificate[], String, SSLEngine)} method.
   *
   * This controls the certificate check used by Druid's internal client, used to validate the certificates of other Druid services.
   *
   * @param chain See docs for {@link X509ExtendedTrustManager#checkServerTrusted(X509Certificate[], String, SSLEngine)}.
   * @param authType See docs for {@link X509ExtendedTrustManager#checkServerTrusted(X509Certificate[], String, SSLEngine)}.
   * @param engine See docs for {@link X509ExtendedTrustManager#checkServerTrusted(X509Certificate[], String, SSLEngine)}.
   * @param baseTrustManager The base trust manager. An extension should call
   *                         baseTrustManager.checkServerTrusted(chain, authType, engine) if/when it wishes
   *                         to use the standard check in addition to custom checks.
   * @throws CertificateException
   */
  void checkServer(
      X509Certificate[] chain,
      String authType,
      SSLEngine engine,
      X509ExtendedTrustManager baseTrustManager
  ) throws CertificateException;
}
