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

import org.apache.druid.error.DruidException;
import org.apache.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.Test;

public class KerberosAuthenticatorTest
{
  private static final String TEST_SERVER_PRINCIPAL = "HTTP/localhost@EXAMPLE.COM";
  private static final String TEST_SERVER_KEYTAB = "/path/to/keytab";
  private static final String TEST_AUTH_TO_LOCAL = "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//";
  private static final String TEST_AUTHORIZER_NAME = "testAuthorizer";
  private static final String TEST_NAME = "testKerberos";
  private static final String TEST_COOKIE_SECRET = "test-secret-key";

  private DruidNode createTestNode()
  {
    return new DruidNode("test", "localhost", false, 8080, null, true, false);
  }


  @Test
  public void testConstructorWithNullCookieSignatureSecret()
  {
    DruidNode node = createTestNode();

    DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> new KerberosAuthenticator(
            TEST_SERVER_PRINCIPAL,
            TEST_SERVER_KEYTAB,
            TEST_AUTH_TO_LOCAL,
            null, // null cookie signature secret
            TEST_AUTHORIZER_NAME,
            TEST_NAME,
            node
        )
    );

    Assert.assertEquals(DruidException.Persona.OPERATOR, exception.getTargetPersona());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, exception.getCategory());
    Assert.assertTrue(
        "Exception message should mention cookieSignatureSecret",
        exception.getMessage().contains("cookieSignatureSecret")
    );
    Assert.assertTrue(
        "Exception message should mention 'is not set'",
        exception.getMessage().contains("is not set")
    );
  }

  @Test
  public void testConstructorWithEmptyCookieSignatureSecret()
  {
    DruidNode node = createTestNode();

    DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> new KerberosAuthenticator(
            TEST_SERVER_PRINCIPAL,
            TEST_SERVER_KEYTAB,
            TEST_AUTH_TO_LOCAL,
            "", // empty cookie signature secret
            TEST_AUTHORIZER_NAME,
            TEST_NAME,
            node
        )
    );

    Assert.assertEquals(DruidException.Persona.OPERATOR, exception.getTargetPersona());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, exception.getCategory());
    Assert.assertTrue(
        "Exception message should mention cookieSignatureSecret",
        exception.getMessage().contains("cookieSignatureSecret")
    );
    Assert.assertTrue(
        "Exception message should mention 'is not set'",
        exception.getMessage().contains("is not set")
    );
  }
}
