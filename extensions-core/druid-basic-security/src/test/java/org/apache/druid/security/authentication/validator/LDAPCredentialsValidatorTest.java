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
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import javax.naming.spi.InitialContextFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
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
    validator.validateCredentials("ldap", "ldap", "validUser", "password".toCharArray());
  }

  private static final BasicAuthLDAPConfig LDAP_CONFIG_WITH_GROUP_SEARCH = new BasicAuthLDAPConfig(
      "ldaps://my-ldap-url",
      "bindUser",
      new DefaultPasswordProvider("bindPassword"),
      "",
      "",
      "",
      BasicAuthUtils.DEFAULT_KEY_ITERATIONS,
      BasicAuthUtils.DEFAULT_CREDENTIAL_VERIFY_DURATION_SECONDS,
      BasicAuthUtils.DEFAULT_CREDENTIAL_MAX_DURATION_SECONDS,
      BasicAuthUtils.DEFAULT_CREDENTIAL_CACHE_SIZE,
      "ou=Groups,dc=example,dc=org",
      "(uniqueMember=%s)"
  );

  @Test
  public void testValidateCredentialsWithGroupSearch()
  {
    Properties properties = new Properties();
    properties.put(Context.INITIAL_CONTEXT_FACTORY, MockGroupSearchContextFactory.class.getName());
    LDAPCredentialsValidator validator = new LDAPCredentialsValidator(
        LDAP_CONFIG_WITH_GROUP_SEARCH,
        new LDAPCredentialsValidator.LruBlockCache(
            3600,
            3600,
            100
        ),
        properties
    );
    final org.apache.druid.server.security.AuthenticationResult result =
        validator.validateCredentials("ldap", "ldap", "validUser", "password".toCharArray());
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getContext());

    final Object searchResultObj = result.getContext().get(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY);
    Assert.assertNotNull(searchResultObj);
    Assert.assertTrue(searchResultObj instanceof SearchResult);

    final SearchResult sr = (SearchResult) searchResultObj;
    final Attribute memberOf = sr.getAttributes().get("memberOf");
    Assert.assertNotNull("memberOf should be populated by reverse group search", memberOf);
    Assert.assertEquals(2, memberOf.size());
  }

  public static class MockGroupSearchContextFactory implements InitialContextFactory
  {
    @SuppressWarnings("BanJNDI")
    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException
    {
      final LdapContext context = Mockito.mock(LdapContext.class);

      // User search result with no memberOf attribute
      final String encodedUsername = LDAPCredentialsValidator.encodeForLDAP("validUser", true);
      final BasicAttributes userAttrs = new BasicAttributes(true);
      userAttrs.put("uid", "validUser");
      final SearchResult userResult = new SearchResult(
          "uid=validUser,ou=Users,dc=example,dc=org",
          null,
          userAttrs
      );
      userResult.setNameInNamespace("uid=validUser,ou=Users,dc=example,dc=org");
      final Iterator<SearchResult> userResults = Collections.singletonList(userResult).iterator();

      Mockito.when(
          context.search(
              ArgumentMatchers.eq(LDAP_CONFIG_WITH_GROUP_SEARCH.getBaseDn()),
              ArgumentMatchers.eq(StringUtils.format(LDAP_CONFIG_WITH_GROUP_SEARCH.getUserSearch(), encodedUsername)),
              ArgumentMatchers.any(SearchControls.class))
      ).thenReturn(makeNamingEnum(userResults));

      // Group search results
      final SearchResult group1 = new SearchResult("cn=admins,ou=Groups,dc=example,dc=org", null, new BasicAttributes(true));
      group1.setNameInNamespace("cn=admins,ou=Groups,dc=example,dc=org");
      final SearchResult group2 = new SearchResult("cn=developers,ou=Groups,dc=example,dc=org", null, new BasicAttributes(true));
      group2.setNameInNamespace("cn=developers,ou=Groups,dc=example,dc=org");
      final List<SearchResult> groupList = Arrays.asList(group1, group2);
      final Iterator<SearchResult> groupResults = groupList.iterator();

      final String escapedDn = LDAPCredentialsValidator.encodeForLDAP("uid=validUser,ou=Users,dc=example,dc=org", true);
      Mockito.when(
          context.search(
              ArgumentMatchers.eq(LDAP_CONFIG_WITH_GROUP_SEARCH.getGroupBaseDn()),
              ArgumentMatchers.eq(StringUtils.format(LDAP_CONFIG_WITH_GROUP_SEARCH.getGroupSearch(), escapedDn)),
              ArgumentMatchers.any(SearchControls.class))
      ).thenReturn(makeNamingEnum(groupResults));

      return context;
    }
  }

  private static NamingEnumeration<SearchResult> makeNamingEnum(final Iterator<SearchResult> iter)
  {
    return new NamingEnumeration<SearchResult>()
    {
      @Override
      public SearchResult next()
      {
        return iter.next();
      }

      @Override
      public boolean hasMore()
      {
        return iter.hasNext();
      }

      @Override
      public void close()
      {
      }

      @Override
      public boolean hasMoreElements()
      {
        return iter.hasNext();
      }

      @Override
      public SearchResult nextElement()
      {
        return iter.next();
      }
    };
  }

  public static class MockContextFactory implements InitialContextFactory
  {
    @SuppressWarnings("BanJNDI")
    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException
    {
      final LdapContext context = Mockito.mock(LdapContext.class);

      final String encodedUsername = LDAPCredentialsValidator.encodeForLDAP("validUser", true);
      final SearchResult result = Mockito.mock(SearchResult.class);
      Mockito.when(result.getNameInNamespace()).thenReturn("uid=user,ou=Users,dc=example,dc=org");
      final Iterator<SearchResult> results = Collections.singletonList(result).iterator();

      Mockito.when(
          context.search(
              ArgumentMatchers.eq(LDAP_CONFIG.getBaseDn()),
              ArgumentMatchers.eq(StringUtils.format(LDAP_CONFIG.getUserSearch(), encodedUsername)),
              ArgumentMatchers.any(SearchControls.class))
      ).thenReturn(makeNamingEnum(results));

      return context;
    }
  }


}
