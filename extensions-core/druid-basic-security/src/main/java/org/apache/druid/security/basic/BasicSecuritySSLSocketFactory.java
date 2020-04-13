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

package org.apache.druid.security.basic;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.TLSCertificateChecker;
import org.apache.druid.server.security.TLSUtils;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

public class BasicSecuritySSLSocketFactory extends SSLSocketFactory
{
  private static final Logger LOG = new Logger(BasicSecuritySSLSocketFactory.class);
  private SSLSocketFactory sf;

  @Inject
  private static BasicAuthSSLConfig basicAuthSSLConfig;
  @Inject
  private static TLSCertificateChecker certificateChecker;

  public BasicSecuritySSLSocketFactory()
  {
    SSLContext ctx = new TLSUtils.ClientSSLContextBuilder()
        .setProtocol(basicAuthSSLConfig.getProtocol())
        .setTrustStoreType(basicAuthSSLConfig.getTrustStoreType())
        .setTrustStorePath(basicAuthSSLConfig.getTrustStorePath())
        .setTrustStoreAlgorithm(basicAuthSSLConfig.getTrustStoreAlgorithm())
        .setTrustStorePasswordProvider(basicAuthSSLConfig.getTrustStorePasswordProvider())
        .setKeyStoreType(basicAuthSSLConfig.getKeyStoreType())
        .setKeyStorePath(basicAuthSSLConfig.getKeyStorePath())
        .setKeyStoreAlgorithm(basicAuthSSLConfig.getKeyManagerFactoryAlgorithm())
        .setCertAlias(basicAuthSSLConfig.getCertAlias())
        .setKeyStorePasswordProvider(basicAuthSSLConfig.getKeyStorePasswordProvider())
        .setKeyManagerFactoryPasswordProvider(basicAuthSSLConfig.getKeyManagerPasswordProvider())
        .setValidateHostnames(basicAuthSSLConfig.getValidateHostnames())
        .setCertificateChecker(certificateChecker)
        .build();

    sf = ctx.getSocketFactory();
  }

  public static SocketFactory getDefault()
  {
    return new BasicSecuritySSLSocketFactory();
  }

  @Override
  public String[] getDefaultCipherSuites()
  {
    return sf.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites()
  {
    return sf.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(
      Socket s,
      String host,
      int port,
      boolean autoClose) throws IOException
  {
    return sf.createSocket(s, host, port, autoClose);
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException
  {
    return sf.createSocket(host, port);
  }

  @Override
  public Socket createSocket(
      String host,
      int port,
      InetAddress localHost,
      int localPort) throws IOException
  {
    return sf.createSocket(host, port, localHost, localPort);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException
  {
    return sf.createSocket(host, port);
  }

  @Override
  public Socket createSocket(
      InetAddress address,
      int port,
      InetAddress localAddress,
      int localPort) throws IOException
  {
    return sf.createSocket(address, port, localAddress, localPort);
  }
}
