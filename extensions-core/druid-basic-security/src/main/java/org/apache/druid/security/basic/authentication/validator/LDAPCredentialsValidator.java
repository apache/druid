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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.security.basic.BasicAuthLDAPConfig;
import org.apache.druid.security.basic.BasicAuthSSLConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityAuthenticationException;
import org.apache.druid.security.basic.authentication.BasicAuthenticatorUserPrincipal;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.TLSCertificateChecker;
import org.apache.druid.server.security.TLSUtils;

import javax.annotation.Nullable;
import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

@JsonTypeName("ldap")
public class LDAPCredentialsValidator implements CredentialsValidator
{
  private static final Logger LOG = new Logger(LDAPCredentialsValidator.class);
  private static final ReentrantLock LOCK = new ReentrantLock();

  private static BasicAuthSSLConfig basicAuthSSLConfig;
  private static TLSCertificateChecker certificateChecker;

  private final LruBlockCache cache;

  private final BasicAuthLDAPConfig ldapConfig;

  @JsonCreator
  public LDAPCredentialsValidator(
      @JacksonInject BasicAuthSSLConfig basicAuthSSLConfig,
      @JacksonInject TLSCertificateChecker tlsCertificateChecker,
      @JsonProperty("url") String url,
      @JsonProperty("bindUser") String bindUser,
      @JsonProperty("bindPassword") PasswordProvider bindPassword,
      @JsonProperty("baseDn") String baseDn,
      @JsonProperty("userSearch") String userSearch,
      @JsonProperty("userAttribute") String userAttribute,
      @JsonProperty("groupFilters") String[] groupFilters,
      @JsonProperty("credentialIterations") Integer credentialIterations,
      @JsonProperty("credentialVerifyDuration") Integer credentialVerifyDuration,
      @JsonProperty("credentialMaxDuration") Integer credentialMaxDuration,
      @JsonProperty("credentialCacheSize") Integer credentialCacheSize
  )
  {
    LDAPCredentialsValidator.basicAuthSSLConfig = basicAuthSSLConfig;
    LDAPCredentialsValidator.certificateChecker = tlsCertificateChecker;
    this.ldapConfig = new BasicAuthLDAPConfig(
        url,
        bindUser,
        bindPassword,
        baseDn,
        userSearch,
        userAttribute,
        groupFilters,
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
    Set<LdapName> groups;
    LdapName userDn;
    Map<String, Object> contexMap = new HashMap<>();

    BasicAuthenticatorUserPrincipal principal = this.cache.getOrExpire(username);
    if (principal != null && principal.hasSameCredentials(password)) {
      contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, principal.getGroups());
      return new AuthenticationResult(username, authorizerName, authenticatorName, contexMap);
    } else {
      try {
        InitialDirContext dirContext = new InitialDirContext(bindProperties(this.ldapConfig));

        try {
          SearchResult userResult = getLdapUserObject(this.ldapConfig, dirContext, username);
          if (userResult == null) {
            LOG.debug("User not found: %s", username);
            return null;
          }
          userDn = new LdapName(userResult.getNameInNamespace());
          groups = getGroupsFromLdap(this.ldapConfig, userResult);
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

      if (!validatePassword(this.ldapConfig, userDn, password)) {
        LOG.debug("Password incorrect for user %s", username);
        throw new BasicSecurityAuthenticationException("User LDAP authentication failed username[%s].", userDn.toString());
      }

      byte[] salt = BasicAuthUtils.generateSalt();
      byte[] hash = BasicAuthUtils.hashPassword(password, salt, this.ldapConfig.getCredentialIterations());
      BasicAuthenticatorUserPrincipal newPrincipal = new BasicAuthenticatorUserPrincipal(
          username,
          new BasicAuthenticatorCredentials(salt, hash, this.ldapConfig.getCredentialIterations()),
          groups
      );

      this.cache.put(username, newPrincipal);
      contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, groups);
      return new AuthenticationResult(username, authorizerName, authenticatorName, contexMap);
    }
  }

  @Nullable
  SearchResult getLdapUserObject(BasicAuthLDAPConfig ldapConfig, DirContext context, String username)
  {
    try {
      SearchControls sc = new SearchControls();
      sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
      sc.setReturningAttributes(new String[] {ldapConfig.getUserAttribute(), "memberOf" });
      NamingEnumeration<SearchResult> results = context.search(
          ldapConfig.getBaseDn(),
          StringUtils.format(ldapConfig.getUserSearch(), username),
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

  Set<LdapName> getGroupsFromLdap(BasicAuthLDAPConfig ldapConfig, SearchResult userResult) throws NamingException
  {
    Set<LdapName> groups = new TreeSet<>();

    Attribute memberOf = userResult.getAttributes().get("memberOf");
    if (memberOf == null) {
      LOG.debug("No memberOf attributes");
      return groups; // not part of any groups
    }

    Set<String> groupFilters = new TreeSet<>(Arrays.asList(ldapConfig.getGroupFilters()));
    for (int i = 0; i < memberOf.size(); i++) {
      String memberDn = memberOf.get(i).toString();
      LdapName ln;
      try {
        ln = new LdapName(memberDn);
      }
      catch (InvalidNameException e) {
        LOG.debug("Invalid LDAP name: %s", memberDn);
        continue;
      }

      if (allowedLdapGroup(ln, groupFilters)) {
        groups.add(ln);
      }

      // valid group name, get roles for it
      // addGroups(groups, ln);
    }

    return groups;
  }

  boolean allowedLdapGroup(LdapName groupName, Set<String> groupFilters)
  {
    for (String filter : groupFilters) {
      try {
        if (filter.startsWith("*,")) {
          LdapName ln = new LdapName(filter.substring(2));
          if (groupName.startsWith(ln)) {
            return true;
          }
        } else if (filter.endsWith(",*")) {
          LdapName ln = new LdapName(filter.substring(0, filter.length() - 2));
          if (groupName.endsWith(ln)) {
            return true;
          }
        } else {
          LOG.debug("Attempting exact filter %s", filter);
          LdapName ln = new LdapName(filter);
          if (groupName.equals(ln)) {
            return true;
          }
        }
      }
      catch (InvalidNameException e) {
        throw new RE(StringUtils.format("Configuration problem - Invalid groupFilter '%s'", filter));
      }
    }
    return false;
  }

  boolean validatePassword(BasicAuthLDAPConfig ldapConfig, LdapName userDn, char[] password)
  {
    InitialDirContext context = null;

    try {
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
    }
  }

  private static class LruBlockCache extends LinkedHashMap<String, BasicAuthenticatorUserPrincipal>
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
    protected boolean removeEldestEntry(Map.Entry<String, BasicAuthenticatorUserPrincipal> eldest)
    {
      return size() > cacheSize;
    }

    @Nullable
    BasicAuthenticatorUserPrincipal getOrExpire(String identity)
    {
      try {
        LOCK.lock();
        BasicAuthenticatorUserPrincipal principal = get(identity);
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
    public BasicAuthenticatorUserPrincipal put(String key, BasicAuthenticatorUserPrincipal value)
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

  public static class BasicSecuritySSLSocketFactory extends SSLSocketFactory
  {
    private static final Logger LOG = new Logger(BasicSecuritySSLSocketFactory.class);
    private SSLSocketFactory sf;

    @Inject
    TLSCertificateChecker certificateChecker2;

    public BasicSecuritySSLSocketFactory()
    {
      SSLContext ctx = new TLSUtils.ClientSSLContextBuilder()
          .setProtocol(basicAuthSSLConfig.getProtocol())
          .setTrustStoreType(basicAuthSSLConfig.getTrustStoreType())
          .setTrustStorePath(basicAuthSSLConfig.getTrustStorePath())
          .setTrustStoreAlgorithm(basicAuthSSLConfig.getTrustStoreAlgorithm())
          .setTrustStorePasswordProvider(basicAuthSSLConfig.getTrustStorePasswordProvider())
          .setKeyStoreType(basicAuthSSLConfig.getKeyStoreType())
          .setKeyStorePath(basicAuthSSLConfig.getKeyStorePath())
          .setKeyStoreAlgorithm(basicAuthSSLConfig.getKeyManagerFactoryAlgorithm())
          .setCertAlias(basicAuthSSLConfig.getCertAlias())
          .setKeyStorePasswordProvider(basicAuthSSLConfig.getKeyStorePasswordProvider())
          .setKeyManagerFactoryPasswordProvider(basicAuthSSLConfig.getKeyManagerPasswordProvider())
          .setValidateHostnames(basicAuthSSLConfig.getValidateHostnames())
          .setCertificateChecker(certificateChecker)
          .build();

      sf = ctx.getSocketFactory();
    }

    public static SocketFactory getDefault()
    {
      return new BasicSecuritySSLSocketFactory();
    }

    @Override
    public String[] getDefaultCipherSuites()
    {
      return sf.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites()
    {
      return sf.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(
        Socket s,
        String host,
        int port,
        boolean autoClose) throws IOException
    {
      return sf.createSocket(s, host, port, autoClose);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException
    {
      return sf.createSocket(host, port);
    }

    @Override
    public Socket createSocket(
        String host,
        int port,
        InetAddress localHost,
        int localPort) throws IOException
    {
      return sf.createSocket(host, port, localHost, localPort);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException
    {
      return sf.createSocket(host, port);
    }

    @Override
    public Socket createSocket(
        InetAddress address,
        int port,
        InetAddress localAddress,
        int localPort) throws IOException
    {
      return sf.createSocket(address, port, localAddress, localPort);
    }
  }
}
