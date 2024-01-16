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

package org.apache.druid.security.basic.authentication.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.security.basic.BasicAuthLDAPConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityAuthenticationException;
import org.apache.druid.security.basic.BasicSecuritySSLSocketFactory;
import org.apache.druid.security.basic.authentication.LdapUserPrincipal;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;

import javax.annotation.Nullable;
import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

@JsonTypeName("ldap")
public class LDAPCredentialsValidator implements CredentialsValidator
{
  private static final Logger LOG = new Logger(LDAPCredentialsValidator.class);
  private static final ReentrantLock LOCK = new ReentrantLock();

  private final LruBlockCache cache;
  private final PasswordHashGenerator hashGenerator = new PasswordHashGenerator();

  private final BasicAuthLDAPConfig ldapConfig;
  // Custom overrides that can be passed via tests
  @Nullable
  private final Properties overrideProperties;

  @JsonCreator
  public LDAPCredentialsValidator(
      @JsonProperty("url") String url,
      @JsonProperty("bindUser") String bindUser,
      @JsonProperty("bindPassword") PasswordProvider bindPassword,
      @JsonProperty("baseDn") String baseDn,
      @JsonProperty("userSearch") String userSearch,
      @JsonProperty("userAttribute") String userAttribute,
      @JsonProperty("credentialIterations") Integer credentialIterations,
      @JsonProperty("credentialVerifyDuration") Integer credentialVerifyDuration,
      @JsonProperty("credentialMaxDuration") Integer credentialMaxDuration,
      @JsonProperty("credentialCacheSize") Integer credentialCacheSize
  )
  {
    this.ldapConfig = new BasicAuthLDAPConfig(
        url,
        bindUser,
        bindPassword,
        baseDn,
        userSearch,
        userAttribute,
        credentialIterations == null ? BasicAuthUtils.DEFAULT_KEY_ITERATIONS : credentialIterations,
        credentialVerifyDuration == null ? BasicAuthUtils.DEFAULT_CREDENTIAL_VERIFY_DURATION_SECONDS : credentialVerifyDuration,
        credentialMaxDuration == null ? BasicAuthUtils.DEFAULT_CREDENTIAL_MAX_DURATION_SECONDS : credentialMaxDuration,
        credentialCacheSize == null ? BasicAuthUtils.DEFAULT_CREDENTIAL_CACHE_SIZE : credentialCacheSize
    );

    this.cache = new LruBlockCache(
        this.ldapConfig.getCredentialCacheSize(),
        this.ldapConfig.getCredentialVerifyDuration(),
        this.ldapConfig.getCredentialMaxDuration()
    );
    this.overrideProperties = null;
  }

  @VisibleForTesting
  public LDAPCredentialsValidator(
      final BasicAuthLDAPConfig ldapConfig,
      final LruBlockCache cache,
      final Properties overrideProperties
  )
  {
    this.ldapConfig = ldapConfig;
    this.cache = cache;
    this.overrideProperties = overrideProperties;
  }

  Properties bindProperties(BasicAuthLDAPConfig ldapConfig)
  {
    Properties properties = commonProperties(ldapConfig);
    properties.put(Context.SECURITY_PRINCIPAL, ldapConfig.getBindUser());
    properties.put(Context.SECURITY_CREDENTIALS, ldapConfig.getBindPassword().getPassword());
    return properties;
  }

  Properties userProperties(BasicAuthLDAPConfig ldapConfig, LdapName userDn, char[] password)
  {
    Properties properties = commonProperties(ldapConfig);
    properties.put(Context.SECURITY_PRINCIPAL, userDn.toString());
    properties.put(Context.SECURITY_CREDENTIALS, String.valueOf(password));
    return properties;
  }

  Properties commonProperties(BasicAuthLDAPConfig ldapConfig)
  {
    Properties properties = new Properties();
    properties.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    properties.put(Context.PROVIDER_URL, ldapConfig.getUrl());
    properties.put(Context.SECURITY_AUTHENTICATION, "simple");
    if (StringUtils.toLowerCase(ldapConfig.getUrl()).startsWith("ldaps://")) {
      properties.put(Context.SECURITY_PROTOCOL, "ssl");
      properties.put("java.naming.ldap.factory.socket", BasicSecuritySSLSocketFactory.class.getName());
    }
    if (null != overrideProperties) {
      properties.putAll(overrideProperties);
    }
    return properties;
  }

  @Override
  public AuthenticationResult validateCredentials(
      String authenticatorName,
      String authorizerName,
      String username,
      char[] password
  )
  {
    SearchResult userResult;
    LdapName userDn;
    Map<String, Object> contextMap = new HashMap<>();

    LdapUserPrincipal principal = this.cache.getOrExpire(username);
    if (principal != null && principal.hasSameCredentials(password)) {
      contextMap.put(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY, principal.getSearchResult());
      return new AuthenticationResult(username, authorizerName, authenticatorName, contextMap);
    } else {
      ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        // Set the context classloader same as the loader of this class so that BasicSecuritySSLSocketFactory
        // class can be found
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        InitialDirContext dirContext = new InitialDirContext(bindProperties(this.ldapConfig));
        try {
          userResult = getLdapUserObject(this.ldapConfig, dirContext, username);
          if (userResult == null) {
            LOG.debug("User not found: %s", username);
            return null;
          }
          userDn = new LdapName(userResult.getNameInNamespace());
        }
        finally {
          try {
            dirContext.close();
          }
          catch (Exception ignored) {
            // ignored
          }
        }
      }
      catch (NamingException e) {
        LOG.error(e, "Exception during user lookup");
        return null;
      }
      finally {
        Thread.currentThread().setContextClassLoader(currentClassLoader);
      }

      if (!validatePassword(this.ldapConfig, userDn, password)) {
        LOG.debug("Password incorrect for LDAP user %s", username);
        throw new BasicSecurityAuthenticationException(Access.DEFAULT_ERROR_MESSAGE);
      }

      byte[] salt = BasicAuthUtils.generateSalt();
      byte[] hash = hashGenerator.getOrComputePasswordHash(password, salt, this.ldapConfig.getCredentialIterations());
      LdapUserPrincipal newPrincipal = new LdapUserPrincipal(
          username,
          new BasicAuthenticatorCredentials(salt, hash, this.ldapConfig.getCredentialIterations()),
          userResult
      );

      this.cache.put(username, newPrincipal);
      contextMap.put(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY, userResult);
      return new AuthenticationResult(username, authorizerName, authenticatorName, contextMap);
    }
  }

  /**
   * Retrieves an LDAP user object by using {@link javax.naming.ldap.LdapContext#search(Name, String, SearchControls)}.
   *
   * Regarding the "BanJNDI" suppression: Errorprone flags all usage of APIs that may do JNDI lookups because of the
   * potential for RCE. The risk is that an attacker with ability to set user-level properties on the LDAP server could
   * cause us to read a serialized Java object (a well-known security risk). We mitigate the risk by avoiding the
   * "lookup" API, and using the "search" API *without* setting the returningObjFlag.
   *
   * See https://errorprone.info/bugpattern/BanJNDI for more details.
   */
  @SuppressWarnings("BanJNDI")
  @Nullable
  SearchResult getLdapUserObject(BasicAuthLDAPConfig ldapConfig, DirContext context, String username)
  {
    try {
      SearchControls sc = new SearchControls();
      sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
      sc.setReturningAttributes(new String[] {ldapConfig.getUserAttribute(), "memberOf" });
      String encodedUsername = encodeForLDAP(username, true);
      NamingEnumeration<SearchResult> results = context.search(
          ldapConfig.getBaseDn(),
          StringUtils.format(ldapConfig.getUserSearch(), encodedUsername),
          sc);
      try {
        if (!results.hasMore()) {
          return null;
        }
        return results.next();
      }
      finally {
        results.close();
      }
    }
    catch (NamingException e) {
      LOG.debug(e, "Unable to find user '%s'", username);
      return null;
    }
  }

  boolean validatePassword(BasicAuthLDAPConfig ldapConfig, LdapName userDn, char[] password)
  {
    InitialDirContext context = null;
    ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

    try {
      Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
      context = new InitialDirContext(userProperties(ldapConfig, userDn, password));
      return true;
    }
    catch (AuthenticationException e) {
      return false;
    }
    catch (NamingException e) {
      LOG.error(e, "Exception during LDAP authentication username[%s]", userDn.toString());
      return false;
    }
    finally {
      try {
        if (context != null) {
          context.close();
        }
      }
      catch (Exception ignored) {
        LOG.warn("Exception closing LDAP context");
        // ignored
      }
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }

  public static class LruBlockCache extends LinkedHashMap<String, LdapUserPrincipal>
  {
    private static final long serialVersionUID = 7509410739092012261L;

    private final int cacheSize;
    private final int duration;
    private final int maxDuration;

    public LruBlockCache(int cacheSize, int duration, int maxDuration)
    {
      super(16, 0.75f, true);
      this.cacheSize = cacheSize;
      this.duration = duration;
      this.maxDuration = maxDuration;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, LdapUserPrincipal> eldest)
    {
      return size() > cacheSize;
    }

    @Nullable
    LdapUserPrincipal getOrExpire(String identity)
    {
      try {
        LOCK.lock();
        LdapUserPrincipal principal = get(identity);
        if (principal != null) {
          if (principal.isExpired(duration, maxDuration)) {
            remove(identity);
            return null;
          } else {
            return principal;
          }
        } else {
          return null;
        }
      }
      finally {
        LOCK.unlock();
      }
    }

    @Override
    public LdapUserPrincipal put(String key, LdapUserPrincipal value)
    {
      try {
        LOCK.lock();
        return super.put(key, value);
      }
      finally {
        LOCK.unlock();
      }
    }
  }

  /**
   * This code is adapted from DefaultEncoder from version 2.2.0.0 of ESAPI (https://github.com/ESAPI/esapi-java-legacy)
   */
  public static String encodeForLDAP(String input, boolean encodeWildcards)
  {
    if (input == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);

      switch (c) {
        case '\\':
          sb.append("\\5c");
          break;
        case '/':
          sb.append(("\\2f"));
          break;
        case '*':
          if (encodeWildcards) {
            sb.append("\\2a");
          } else {
            sb.append(c);
          }

          break;
        case '(':
          sb.append("\\28");
          break;
        case ')':
          sb.append("\\29");
          break;
        case '\0':
          sb.append("\\00");
          break;
        default:
          sb.append(c);
      }
    }
    return sb.toString();
  }

}
