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

import org.apache.druid.java.util.common.logger.Logger;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public class BasicSecuritySSLSocketFactory extends SSLSocketFactory
{
  private static final Logger LOG = new Logger(BasicSecuritySSLSocketFactory.class);
  private SSLSocketFactory sf;

  public BasicSecuritySSLSocketFactory()
  {
    try {
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(
          null,
          new TrustManager[] {new BasicSecurityTrustManager()},
          new SecureRandom());
      sf = ctx.getSocketFactory();
    }
    catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new IllegalArgumentException(
          "Unable to initialize socket factory", e);
    }
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

  static class BasicSecurityTrustManager implements X509TrustManager
  {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
    {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
    {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers()
    {
      return new X509Certificate[0];
    }
  }

}
