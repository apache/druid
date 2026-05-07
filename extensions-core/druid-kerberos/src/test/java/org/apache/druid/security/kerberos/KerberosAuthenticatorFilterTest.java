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

package org.apache.druid.security.kerberos;

import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.junit.Assert;
import org.junit.Test;

public class KerberosAuthenticatorFilterTest
{
  @Test
  public void testAuthenticationTokenGetUserNameReturnsShortName()
  {
    final String shortName = "druid";
    final String fullPrincipal = "druid@EXAMPLE.COM";
    final AuthenticationToken token = new AuthenticationToken(shortName, fullPrincipal, "kerberos");

    Assert.assertEquals(shortName, token.getUserName());
    Assert.assertEquals(fullPrincipal, token.getName());
  }

  @Test
  public void testAuthenticationResultUsesShortName()
  {
    final String shortName = "druid";
    final String fullPrincipal = "druid@EXAMPLE.COM";
    final String authorizerName = "testAuthorizer";
    final String authenticatorName = "kerberos";

    final AuthenticationToken token = new AuthenticationToken(shortName, fullPrincipal, "kerberos");

    // This mirrors the fix: using getUserName() instead of getName()
    final AuthenticationResult result = new AuthenticationResult(
        token.getUserName(),
        authorizerName,
        authenticatorName,
        null
    );

    Assert.assertEquals(
        "Identity should be the short name, not the full Kerberos principal",
        shortName,
        result.getIdentity()
    );
    Assert.assertNotEquals(
        "Identity should not be the full Kerberos principal",
        fullPrincipal,
        result.getIdentity()
    );
  }

  @Test
  public void testAuthenticationResultWithMultiComponentPrincipal()
  {
    final String shortName = "druid";
    final String fullPrincipal = "druid/admin@EXAMPLE.COM";
    final String authorizerName = "testAuthorizer";
    final String authenticatorName = "kerberos";

    final AuthenticationToken token = new AuthenticationToken(shortName, fullPrincipal, "kerberos");

    final AuthenticationResult result = new AuthenticationResult(
        token.getUserName(),
        authorizerName,
        authenticatorName,
        null
    );

    Assert.assertEquals(shortName, result.getIdentity());
  }

  @Test
  public void testGetNameReturnsFullPrincipalNotShortName()
  {
    final String shortName = "user";
    final String fullPrincipal = "user@REALM.COM";
    final AuthenticationToken token = new AuthenticationToken(shortName, fullPrincipal, "kerberos");

    // Verify getName() returns the full principal — this is what was being used before the fix
    Assert.assertEquals(fullPrincipal, token.getName());
    // Verify getUserName() returns the short name — this is what the fix uses
    Assert.assertEquals(shortName, token.getUserName());
  }
}
