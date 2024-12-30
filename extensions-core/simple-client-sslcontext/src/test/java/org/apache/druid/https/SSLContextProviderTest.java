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

package org.apache.druid.https;

import org.apache.druid.server.security.TLSCertificateChecker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SSLContextProviderTest
{
  @Mock
  private SSLClientConfig mockConfig;

  @Mock
  private TLSCertificateChecker mockCertificateChecker;

  @InjectMocks
  private SSLContextProvider sslContextProvider;

  @BeforeEach
  public void setUp()
  {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testGet()
  {
    when(mockConfig.getProtocol()).thenReturn("TLS");
    when(mockConfig.getTrustStoreType()).thenReturn("JKS");
    when(mockConfig.getTrustStorePath()).thenReturn("src/test/resources/keystore.jks");
    when(mockConfig.getTrustStoreAlgorithm()).thenReturn("SunX509");
    when(mockConfig.getTrustStorePasswordProvider()).thenReturn(() -> "changeit");
    when(mockConfig.getKeyStoreType()).thenReturn("JKS");
    when(mockConfig.getKeyStorePath()).thenReturn("src/test/resources/keystore.jks");
    when(mockConfig.getKeyManagerFactoryAlgorithm()).thenReturn("SunX509");
    when(mockConfig.getCertAlias()).thenReturn("myalias");
    when(mockConfig.getKeyStorePasswordProvider()).thenReturn(() -> "changeit");
    when(mockConfig.getKeyManagerPasswordProvider()).thenReturn(() -> "changeit");
    when(mockConfig.getValidateHostnames()).thenReturn(true);

    SSLContext sslContext = sslContextProvider.get();

    assertNotNull(sslContext);
    assertEquals("TLS", sslContext.getProtocol());

    verify(mockConfig).getProtocol();
    verify(mockConfig).getTrustStoreType();
    verify(mockConfig).getTrustStorePath();
    verify(mockConfig).getTrustStoreAlgorithm();
    verify(mockConfig).getTrustStorePasswordProvider();
    verify(mockConfig).getKeyStoreType();
    verify(mockConfig).getKeyStorePath();
    verify(mockConfig).getKeyManagerFactoryAlgorithm();
    verify(mockConfig).getCertAlias();
    verify(mockConfig).getKeyStorePasswordProvider();
    verify(mockConfig).getKeyManagerPasswordProvider();
    verify(mockConfig).getValidateHostnames();
  }

  @Test
  public void testSSLContextProviderConstructor()
  {
    SSLContextProvider provider = new SSLContextProvider(mockConfig, mockCertificateChecker);
    assertNotNull(provider);
  }
}
