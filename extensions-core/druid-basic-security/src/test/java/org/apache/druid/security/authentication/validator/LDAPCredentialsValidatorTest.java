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

package org.apache.druid.security.authentication.validator;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.security.basic.BasicAuthLDAPConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.authentication.validator.LDAPCredentialsValidator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import javax.naming.spi.InitialContextFactory;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

public class LDAPCredentialsValidatorTest
{
  private static final BasicAuthLDAPConfig LDAP_CONFIG = new BasicAuthLDAPConfig(
      "ldaps://my-ldap-url",
      "bindUser",
      new DefaultPasswordProvider("bindPassword"),
      "",
      "",
      "",
      BasicAuthUtils.DEFAULT_KEY_ITERATIONS,
      BasicAuthUtils.DEFAULT_CREDENTIAL_VERIFY_DURATION_SECONDS,
      BasicAuthUtils.DEFAULT_CREDENTIAL_MAX_DURATION_SECONDS,
      BasicAuthUtils.DEFAULT_CREDENTIAL_CACHE_SIZE);

  @Test
  public void testEncodeForLDAP_noSpecialChars()
  {
    String input = "user1";
    String encoded = LDAPCredentialsValidator.encodeForLDAP(input, true);
    Assert.assertEquals(input, encoded);
  }

  @Test
  public void testEncodeForLDAP_specialChars()
  {
    String input = "user1\\*()\0/user1";
    String encodedWildcardTrue = LDAPCredentialsValidator.encodeForLDAP(input, true);
    String encodedWildcardFalse = LDAPCredentialsValidator.encodeForLDAP(input, false);
    String expectedWildcardTrue = "user1\\5c\\2a\\28\\29\\00\\2fuser1";
    String expectedWildcardFalse = "user1\\5c*\\28\\29\\00\\2fuser1";
    Assert.assertEquals(expectedWildcardTrue, encodedWildcardTrue);
    Assert.assertEquals(expectedWildcardFalse, encodedWildcardFalse);
  }

  /**
   * This doesn't test password validation.
   */
  @Test
  public void testValidateCredentials()
  {
    Properties properties = new Properties();
    properties.put(Context.INITIAL_CONTEXT_FACTORY, MockContextFactory.class.getName());
    LDAPCredentialsValidator validator = new LDAPCredentialsValidator(
        LDAP_CONFIG,
        new LDAPCredentialsValidator.LruBlockCache(
            3600,
            3600,
            100
        ),
        properties
    );
    validator.validateCredentials("ldap", "ldap", "validUser", "password".toCharArray());
  }

  public static class MockContextFactory implements InitialContextFactory
  {
    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException
    {
      LdapContext context = Mockito.mock(LdapContext.class);

      String encodedUsername = LDAPCredentialsValidator.encodeForLDAP("validUser", true);
      SearchResult result = Mockito.mock(SearchResult.class);
      Mockito.when(result.getNameInNamespace()).thenReturn("uid=user,ou=Users,dc=example,dc=org");
      Iterator<SearchResult> results = Collections.singletonList(result).iterator();

      Mockito.when(
          context.search(
              ArgumentMatchers.eq(LDAP_CONFIG.getBaseDn()),
              ArgumentMatchers.eq(StringUtils.format(LDAP_CONFIG.getUserSearch(), encodedUsername)),
              ArgumentMatchers.any(SearchControls.class))
      ).thenReturn(new NamingEnumeration<SearchResult>()
      {
        @Override
        public SearchResult next()
        {
          return results.next();
        }

        @Override
        public boolean hasMore()
        {
          return results.hasNext();
        }

        @Override
        public void close()
        {
          // No-op
        }

        @Override
        public boolean hasMoreElements()
        {
          return results.hasNext();
        }

        @Override
        public SearchResult nextElement()
        {
          return results.next();
        }
      });

      return context;
    }
  }


}
