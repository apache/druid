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

package org.apache.druid.testing.utils;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.TLSCertificateChecker;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class ITTLSCertificateChecker implements TLSCertificateChecker
{
  private static final Logger log = new Logger(ITTLSCertificateChecker.class);

  @Override
  public void checkClient(
      X509Certificate[] chain,
      String authType,
      SSLEngine engine,
      X509ExtendedTrustManager baseTrustManager
  ) throws CertificateException
  {
    // only the integration test client with "thisisprobablynottherighthostname" cert is allowed to talk to me
    if (!chain[0].toString().contains("thisisprobablynottherighthostname") || !engine.getPeerHost().contains("172.172.172.1")) {
      throw new CertificateException("Custom check rejected request from client.");
    }
  }

  @Override
  public void checkServer(
      X509Certificate[] chain,
      String authType,
      SSLEngine engine,
      X509ExtendedTrustManager baseTrustManager
  ) throws CertificateException
  {
    baseTrustManager.checkServerTrusted(chain, authType, engine);

    // fail intentionally when trying to talk to the broker
    if (chain[0].toString().contains("172.172.172.60")) {
      throw new CertificateException("Custom check intentionally terminated request to broker.");
    }
  }
}
