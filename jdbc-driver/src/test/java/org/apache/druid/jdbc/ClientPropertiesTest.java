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

package org.apache.druid.jdbc;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Map;

public class ClientPropertiesTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ClientProperties.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testAuthenticationDefaultsToBasicWithUserAndPassword() throws SQLException
  {
    Assertions.assertEquals(
        ClientProperties.AUTHENTICATION_BASIC,
        clientPropertiesOf(Map.of("user", "admin", "password", "secret")).getAuthentication()
    );
  }

  @Test
  public void testExplicitAuthenticationOverridesDefault() throws SQLException
  {
    Assertions.assertEquals(
        ClientProperties.AUTHENTICATION_BASIC_RAW,
        clientPropertiesOf(Map.of("authentication", "basicRaw", "password", "token")).getAuthentication()
    );
  }

  private static ClientProperties clientPropertiesOf(final Map<String, String> allProperties) throws SQLException
  {
    return ClientProperties.splitProperties(allProperties).clientProperties();
  }
}
