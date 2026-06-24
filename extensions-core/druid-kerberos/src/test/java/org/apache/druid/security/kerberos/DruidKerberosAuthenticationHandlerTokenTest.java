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

import org.apache.druid.server.security.AuthenticationResult;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests verifying that AuthenticationToken userName (short name) is used for
 * AuthenticationResult identity, not the full Kerberos principal.
 *
 * <p>The DruidKerberosAuthenticationHandler creates tokens with:
 * {@code new AuthenticationToken(userName, clientPrincipal, getType())}
 * where userName is the result of KerberosName.getShortName() (applying authToLocal rules).
 *
 * <p>The KerberosAuthenticator must use token.getUserName() (not token.getName()) when
 * constructing AuthenticationResult so that the identity matches the short name
 * produced by authToLocal rules.
 */
public class DruidKerberosAuthenticationHandlerTokenTest
{
  private static final String SAFE_RULES = "RULE:[1:$1] RULE:[2:$1]";

  @Before
  public void setUp()
  {
    KerberosName.setRules(SAFE_RULES);
  }

  @After
  public void tearDown()
  {
    KerberosName.setRules(SAFE_RULES);
  }

  @Test
  public void testSimplePrincipalMapping()
  {
    final String shortName = "druid";
    final String principal = "druid@EXAMPLE.COM";
    verifyTokenIdentity(shortName, principal);
  }

  @Test
  public void testServicePrincipalMapping()
  {
    final String shortName = "druid";
    final String principal = "druid/host.example.com@EXAMPLE.COM";
    verifyTokenIdentity(shortName, principal);
  }

  @Test
  public void testPrincipalWithDifferentShortName()
  {
    // Simulates authToLocal rule mapping a principal to a different local name
    final String shortName = "admin";
    final String principal = "admin/secure@CORP.EXAMPLE.COM";
    verifyTokenIdentity(shortName, principal);
  }

  @Test
  public void testAuthToLocalRuleExtractsShortName() throws Exception
  {
    // Use an explicit rule that strips the realm, matching what DEFAULT does with a krb5.conf
    KerberosName.setRules("RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//");
    final KerberosName kerberosName = new KerberosName("user@EXAMPLE.COM");
    final String shortName = kerberosName.getShortName();

    Assert.assertEquals("user", shortName);

    // This is what DruidKerberosAuthenticationHandler does
    final AuthenticationToken token = new AuthenticationToken(shortName, "user@EXAMPLE.COM", "kerberos");

    // And KerberosAuthenticator must use getUserName() for identity
    final AuthenticationResult result = new AuthenticationResult(
        token.getUserName(),
        "authorizer",
        "kerberos",
        null
    );
    Assert.assertEquals("user", result.getIdentity());
  }

  @Test
  public void testCustomAuthToLocalRule() throws Exception
  {
    // Test with a custom rule that maps druid@EXAMPLE.COM -> druid
    KerberosName.setRules("RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*// DEFAULT");
    final KerberosName kerberosName = new KerberosName("myuser@EXAMPLE.COM");
    final String shortName = kerberosName.getShortName();

    Assert.assertEquals("myuser", shortName);

    final AuthenticationToken token = new AuthenticationToken(shortName, "myuser@EXAMPLE.COM", "kerberos");

    final AuthenticationResult result = new AuthenticationResult(
        token.getUserName(),
        "authorizer",
        "kerberos",
        null
    );
    Assert.assertEquals("myuser", result.getIdentity());
  }

  @Test
  public void testTwoComponentPrincipalRule() throws Exception
  {
    // Rule for two-component principals: extract the first component (service name)
    KerberosName.setRules("RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//");
    final KerberosName kerberosName = new KerberosName("HTTP/host.example.com@EXAMPLE.COM");
    final String shortName = kerberosName.getShortName();

    Assert.assertEquals("HTTP", shortName);

    final AuthenticationToken token = new AuthenticationToken(
        shortName,
        "HTTP/host.example.com@EXAMPLE.COM",
        "kerberos"
    );

    final AuthenticationResult result = new AuthenticationResult(
        token.getUserName(),
        "authorizer",
        "kerberos",
        null
    );
    Assert.assertEquals("HTTP", result.getIdentity());
    Assert.assertNotEquals("HTTP/host.example.com@EXAMPLE.COM", result.getIdentity());
  }

  private void verifyTokenIdentity(String expectedShortName, String fullPrincipal)
  {
    final AuthenticationToken token = new AuthenticationToken(expectedShortName, fullPrincipal, "kerberos");

    // getUserName() should return the short name
    Assert.assertEquals(expectedShortName, token.getUserName());
    // getName() should return the full principal
    Assert.assertEquals(fullPrincipal, token.getName());

    // AuthenticationResult identity must use the short name
    final AuthenticationResult result = new AuthenticationResult(
        token.getUserName(),
        "testAuthorizer",
        "kerberos",
        null
    );
    Assert.assertEquals(expectedShortName, result.getIdentity());
  }
}
