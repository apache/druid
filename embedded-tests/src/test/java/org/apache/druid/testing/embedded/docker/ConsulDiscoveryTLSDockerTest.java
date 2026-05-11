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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.testing.embedded.consul.ConsulSecurityMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests Consul service discovery and leader election with TLS (server-side encryption).
 * Server certificate is validated using a truststore, but no client certificate is required.
 * Inherits all common tests from {@link BaseConsulDiscoveryDockerTest}.
 */
public class ConsulDiscoveryTLSDockerTest extends BaseConsulDiscoveryDockerTest
{
  @Override
  protected ConsulSecurityMode getConsulSecurityMode()
  {
    return ConsulSecurityMode.TLS;
  }

  /**
   * TLS-specific test: Verify that certificates were generated and are accessible.
   */
  @Test
  public void testTLSCertificatesGenerated()
  {
    Assertions.assertNotNull(consulResource.getCertificateBundle(), "Certificate bundle should be generated");
    Assertions.assertNotNull(consulResource.getTrustStorePath(), "Truststore path should be available");
    Assertions.assertEquals(ConsulSecurityMode.TLS, consulResource.getSecurityMode());
  }
}
