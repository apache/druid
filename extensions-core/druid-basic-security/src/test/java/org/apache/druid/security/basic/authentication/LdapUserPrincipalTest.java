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

package org.apache.druid.security.basic.authentication;

import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.naming.directory.SearchResult;
import java.time.Instant;

public class LdapUserPrincipalTest
{

  private static final BasicAuthenticatorCredentials USER_CREDENTIALS = new BasicAuthenticatorCredentials(
      new BasicAuthenticatorCredentialUpdate("helloworld", 20)
  );

  private static final long CREATED_MILLIS = 10000;

  // this will create a cache with createdAt now():
  private static final LdapUserPrincipal PRINCIPAL = new LdapUserPrincipal(
      "foo",
      USER_CREDENTIALS,
      new SearchResult("foo", null, null),
      Instant.ofEpochMilli(CREATED_MILLIS)
  );

  @Test
  public void testIsNotExpired()
  {
    Assertions.assertFalse(PRINCIPAL.isExpired(1, 10, CREATED_MILLIS + 500));
  }

  @Test
  public void testIsObviouslyExpired()
  {
    // real clock now should be so much bigger than CREATED_MILLIS....so it must have expired...
    Assertions.assertTrue(PRINCIPAL.isExpired(100, 1000));
  }

  @Test
  public void testIsExpiredWhenMaxDurationIsSmall()
  {
    Assertions.assertTrue(PRINCIPAL.isExpired(10, 1, CREATED_MILLIS + 1001));
  }

  @Test
  public void testIsExpiredWhenDurationIsSmall()
  {
    Assertions.assertTrue(PRINCIPAL.isExpired(1, 10, CREATED_MILLIS + 1001));
  }

  @Test
  public void testIsExpiredWhenDurationsAreSmall()
  {
    Assertions.assertTrue(PRINCIPAL.isExpired(1, 1, CREATED_MILLIS + 1001));
  }
}
