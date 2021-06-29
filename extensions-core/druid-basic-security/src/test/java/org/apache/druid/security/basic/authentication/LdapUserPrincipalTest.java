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

import junit.framework.TestCase;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.junit.Assert;

import javax.naming.directory.SearchResult;

public class LdapUserPrincipalTest extends TestCase
{

  private static final BasicAuthenticatorCredentials USER_CREDENTIALS = new BasicAuthenticatorCredentials(
      new BasicAuthenticatorCredentialUpdate("helloworld", 20)
  );

  // this will create a cache with createdAt now():
  private static final LdapUserPrincipal principal = new LdapUserPrincipal(
      "foo",
      USER_CREDENTIALS,
      new SearchResult("foo", null, null)
  );

  public void testIsNotExpired()
  {
    Assert.assertFalse(principal.isExpired(1000, 1000));
  }

  public void testIsExpiredWhenMaxDurationIsSmall() throws InterruptedException
  {
    Thread.sleep(1000);
    Assert.assertTrue(principal.isExpired(10, 1));
  }

  public void testIsExpiredWhenDurationIsSmall() throws InterruptedException
  {
    Thread.sleep(2000);
    Assert.assertTrue(principal.isExpired(1, 10));
  }

  public void testIsExpiredWhenDurationsAreSmall() throws InterruptedException
  {
    Thread.sleep(1000);
    Assert.assertTrue(principal.isExpired(1, 1));
  }
}
