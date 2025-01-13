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

import org.apache.druid.metadata.PasswordProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SSLClientConfigTest
{
  @Test
  public void testGetters()
  {
    SSLClientConfig sslClientConfig = mock(SSLClientConfig.class);

    String expectedProtocol = "TLS";
    String expectedTrustStoreType = "JKS";
    String expectedTrustStorePath = "/path/to/truststore";
    String expectedTrustStoreAlgorithm = "SunX509";
    PasswordProvider expectedTrustStorePasswordProvider = () -> "trustStorePassword";
    String expectedKeyStorePath = "/path/to/keystore";
    String expectedKeyStoreType = "JKS";
    String expectedCertAlias = "alias";
    PasswordProvider expectedKeyStorePasswordProvider = () -> "keyStorePassword";
    PasswordProvider expectedKeyManagerPasswordProvider = () -> "keyManagerPassword";
    String expectedKeyManagerFactoryAlgorithm = "SunX509";
    Boolean expectedValidateHostnames = true;

    when(sslClientConfig.getProtocol()).thenReturn(expectedProtocol);
    when(sslClientConfig.getTrustStoreType()).thenReturn(expectedTrustStoreType);
    when(sslClientConfig.getTrustStorePath()).thenReturn(expectedTrustStorePath);
    when(sslClientConfig.getTrustStoreAlgorithm()).thenReturn(expectedTrustStoreAlgorithm);
    when(sslClientConfig.getTrustStorePasswordProvider()).thenReturn(expectedTrustStorePasswordProvider);
    when(sslClientConfig.getKeyStorePath()).thenReturn(expectedKeyStorePath);
    when(sslClientConfig.getKeyStoreType()).thenReturn(expectedKeyStoreType);
    when(sslClientConfig.getCertAlias()).thenReturn(expectedCertAlias);
    when(sslClientConfig.getKeyStorePasswordProvider()).thenReturn(expectedKeyStorePasswordProvider);
    when(sslClientConfig.getKeyManagerPasswordProvider()).thenReturn(expectedKeyManagerPasswordProvider);
    when(sslClientConfig.getKeyManagerFactoryAlgorithm()).thenReturn(expectedKeyManagerFactoryAlgorithm);
    when(sslClientConfig.getValidateHostnames()).thenReturn(expectedValidateHostnames);
    when(sslClientConfig.toString()).thenCallRealMethod();

    assertEquals(expectedProtocol, sslClientConfig.getProtocol());
    assertEquals(expectedTrustStoreType, sslClientConfig.getTrustStoreType());
    assertEquals(expectedProtocol, sslClientConfig.getProtocol());
    assertEquals(expectedTrustStoreType, sslClientConfig.getTrustStoreType());
    assertEquals(expectedTrustStorePath, sslClientConfig.getTrustStorePath());
    assertEquals(expectedTrustStoreAlgorithm, sslClientConfig.getTrustStoreAlgorithm());
    assertEquals(expectedTrustStorePasswordProvider, sslClientConfig.getTrustStorePasswordProvider());
    assertEquals(expectedKeyStorePath, sslClientConfig.getKeyStorePath());
    assertEquals(expectedKeyStoreType, sslClientConfig.getKeyStoreType());
    assertEquals(expectedCertAlias, sslClientConfig.getCertAlias());
    assertEquals(expectedKeyStorePasswordProvider, sslClientConfig.getKeyStorePasswordProvider());
    assertEquals(expectedKeyManagerPasswordProvider, sslClientConfig.getKeyManagerPasswordProvider());
    assertEquals(expectedKeyManagerFactoryAlgorithm, sslClientConfig.getKeyManagerFactoryAlgorithm());
    assertEquals(expectedValidateHostnames, sslClientConfig.getValidateHostnames());
  }
}
