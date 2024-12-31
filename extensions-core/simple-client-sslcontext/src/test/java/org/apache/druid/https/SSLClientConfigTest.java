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

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class SSLClientConfigTest
{
  @Test
  public void testGetters() throws NoSuchFieldException, IllegalAccessException
  {
    SSLClientConfig sslClientConfig = new SSLClientConfig();

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

    // Use reflection to set the private fields
    setField(sslClientConfig, "protocol", expectedProtocol);
    setField(sslClientConfig, "trustStoreType", expectedTrustStoreType);
    setField(sslClientConfig, "trustStorePath", expectedTrustStorePath);
    setField(sslClientConfig, "trustStoreAlgorithm", expectedTrustStoreAlgorithm);
    setField(sslClientConfig, "trustStorePasswordProvider", expectedTrustStorePasswordProvider);
    setField(sslClientConfig, "keyStorePath", expectedKeyStorePath);
    setField(sslClientConfig, "keyStoreType", expectedKeyStoreType);
    setField(sslClientConfig, "certAlias", expectedCertAlias);
    setField(sslClientConfig, "keyStorePasswordProvider", expectedKeyStorePasswordProvider);
    setField(sslClientConfig, "keyManagerPasswordProvider", expectedKeyManagerPasswordProvider);
    setField(sslClientConfig, "keyManagerFactoryAlgorithm", expectedKeyManagerFactoryAlgorithm);
    setField(sslClientConfig, "validateHostnames", expectedValidateHostnames);

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
    assertEquals("SSLClientConfig{protocol='TLS', trustStoreType='JKS', trustStorePath='/path/to/truststore', trustStoreAlgorithm='SunX509', keyStorePath='/path/to/keystore', keyStoreType='JKS', certAlias='alias', keyManagerFactoryAlgorithm='SunX509', validateHostnames='true'}", sslClientConfig.toString());
  }

  private void setField(Object object, String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException
  {
    Field field = object.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(object, value);
  }
}
