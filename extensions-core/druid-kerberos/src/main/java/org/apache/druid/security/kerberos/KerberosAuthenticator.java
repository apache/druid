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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerException;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.net.HttpCookie;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;


@JsonTypeName("kerberos")
public class KerberosAuthenticator implements Authenticator
{
  private static final Logger log = new Logger(KerberosAuthenticator.class);
  public static final String SIGNED_TOKEN_ATTRIBUTE = "signedToken";

  private final String serverPrincipal;
  private final String serverKeytab;
  private final String authToLocal;
  private final String cookieSignatureSecret;
  private final String authorizerName;
  private final String name;
  private LoginContext loginContext;

  @JsonCreator
  public KerberosAuthenticator(
      @JsonProperty("serverPrincipal") String serverPrincipal,
      @JsonProperty("serverKeytab") String serverKeytab,
      @JsonProperty("authToLocal") String authToLocal,
      @JsonProperty("cookieSignatureSecret") String cookieSignatureSecret,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("name") String name,
      @JacksonInject @Self DruidNode node
  )
  {
    this.serverKeytab = serverKeytab;
    this.authToLocal = authToLocal == null ? "DEFAULT" : authToLocal;
    this.cookieSignatureSecret = cookieSignatureSecret;
    this.authorizerName = authorizerName;
    this.name = Preconditions.checkNotNull(name);

    try {
      this.serverPrincipal = SecurityUtil.getServerPrincipal(serverPrincipal, node.getHost());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Filter getFilter()
  {
    return new AuthenticationFilter()
    {
      private Signer mySigner;

      @Override
      public void init(FilterConfig filterConfig) throws ServletException
      {
        ClassLoader prevLoader = Thread.currentThread().getContextClassLoader();
        try {
          // AuthenticationHandler is created during Authenticationfilter.init using reflection with thread context class loader.
          // In case of druid since the class is actually loaded as an extension and filter init is done in main thread.
          // We need to set the classloader explicitly to extension class loader.
          Thread.currentThread().setContextClassLoader(AuthenticationFilter.class.getClassLoader());
          super.init(filterConfig);
          String configPrefix = filterConfig.getInitParameter(CONFIG_PREFIX);
          configPrefix = (configPrefix != null) ? configPrefix + "." : "";
          Properties config = getConfiguration(configPrefix, filterConfig);
          String signatureSecret = config.getProperty(configPrefix + SIGNATURE_SECRET);
          if (signatureSecret == null) {
            signatureSecret = Long.toString(ThreadLocalRandom.current().nextLong());
            log.warn("'signature.secret' configuration not set, using a random value as secret");
          }
          final byte[] secretBytes = StringUtils.toUtf8(signatureSecret);
          SignerSecretProvider signerSecretProvider = new SignerSecretProvider()
          {
            @Override
            public void init(Properties config, ServletContext servletContext, long tokenValidity)
            {

            }

            @Override
            public byte[] getCurrentSecret()
            {
              return secretBytes;
            }

            @Override
            public byte[][] getAllSecrets()
            {
              return new byte[][]{secretBytes};
            }
          };
          mySigner = new Signer(signerSecretProvider);
        }
        finally {
          Thread.currentThread().setContextClassLoader(prevLoader);
        }
      }

      // Copied from hadoop-auth's AuthenticationFilter, to allow us to change error response handling in doFilterSuper
      @Override
      protected AuthenticationToken getToken(HttpServletRequest request) throws AuthenticationException
      {
        AuthenticationToken token = null;
        String tokenStr = null;
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
          for (Cookie cookie : cookies) {
            if (cookie.getName().equals(AuthenticatedURL.AUTH_COOKIE)) {
              tokenStr = cookie.getValue();
              try {
                tokenStr = mySigner.verifyAndExtract(tokenStr);
              }
              catch (SignerException ex) {
                throw new AuthenticationException(ex);
              }
              break;
            }
          }
        }
        if (tokenStr != null) {
          token = AuthenticationToken.parse(tokenStr);
          if (!token.getType().equals(getAuthenticationHandler().getType())) {
            throw new AuthenticationException("Invalid AuthenticationToken type");
          }
          if (token.isExpired()) {
            throw new AuthenticationException("AuthenticationToken expired");
          }
        }
        return token;
      }

      @Override
      public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
          throws IOException, ServletException
      {
        // If there's already an auth result, then we have authenticated already, skip this.
        if (request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT) != null) {
          filterChain.doFilter(request, response);
          return;
        }

        // In the hadoop-auth 2.7.3 code that this was adapted from, the login would've occurred during init() of
        // the AuthenticationFilter via `initializeAuthHandler(authHandlerClassName, filterConfig)`.
        // Since we co-exist with other authentication schemes, don't login until we've checked that
        // some other Authenticator didn't already validate this request.
        if (loginContext == null) {
          initializeKerberosLogin();
        }

        // Run the original doFilter method, but with modifications to error handling
        doFilterSuper(request, response, filterChain);
      }


      /**
       * Copied from hadoop-auth 2.7.3 AuthenticationFilter, to allow us to change error response handling.
       * Specifically, we want to defer the sending of 401 Unauthorized so that other Authenticators later in the chain
       * can check the request.
       */
      private void doFilterSuper(ServletRequest request, ServletResponse response, FilterChain filterChain)
          throws IOException, ServletException
      {
        boolean unauthorizedResponse = true;
        int errCode = HttpServletResponse.SC_UNAUTHORIZED;
        AuthenticationException authenticationEx = null;
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        boolean isHttps = "https".equals(httpRequest.getScheme());
        try {
          boolean newToken = false;
          AuthenticationToken token;
          try {
            token = getToken(httpRequest);
          }
          catch (AuthenticationException ex) {
            log.warn("AuthenticationToken ignored: " + ex.getMessage());
            // will be sent back in a 401 unless filter authenticates
            authenticationEx = ex;
            token = null;
          }
          if (getAuthenticationHandler().managementOperation(token, httpRequest, httpResponse)) {
            if (token == null) {
              if (log.isDebugEnabled()) {
                log.debug("Request [{%s}] triggering authentication", getRequestURL(httpRequest));
              }
              token = getAuthenticationHandler().authenticate(httpRequest, httpResponse);
              if (token != null && token.getExpires() != 0 &&
                  token != AuthenticationToken.ANONYMOUS) {
                token.setExpires(System.currentTimeMillis() + getValidity() * 1000);
              }
              newToken = true;
            }
            if (token != null) {
              unauthorizedResponse = false;
              if (log.isDebugEnabled()) {
                log.debug("Request [{%s}] user [{%s}] authenticated", getRequestURL(httpRequest), token.getUserName());
              }
              final AuthenticationToken authToken = token;
              httpRequest = new HttpServletRequestWrapper(httpRequest)
              {

                @Override
                public String getAuthType()
                {
                  return authToken.getType();
                }

                @Override
                public String getRemoteUser()
                {
                  return authToken.getUserName();
                }

                @Override
                public Principal getUserPrincipal()
                {
                  return (authToken != AuthenticationToken.ANONYMOUS) ? authToken : null;
                }
              };
              if (newToken && !token.isExpired() && token != AuthenticationToken.ANONYMOUS) {
                String signedToken = mySigner.sign(token.toString());
                tokenToAuthCookie(
                    httpResponse,
                    signedToken,
                    getCookieDomain(),
                    getCookiePath(),
                    token.getExpires(),
                    !token.isExpired() && token.getExpires() > 0,
                    isHttps
                );
                request.setAttribute(SIGNED_TOKEN_ATTRIBUTE, tokenToCookieString(
                    signedToken,
                    getCookieDomain(),
                    getCookiePath(),
                    token.getExpires(),
                    !token.isExpired() && token.getExpires() > 0,
                    isHttps
                ));
              }
              // Since this request is validated also set DRUID_AUTHENTICATION_RESULT
              request.setAttribute(
                  AuthConfig.DRUID_AUTHENTICATION_RESULT,
                  new AuthenticationResult(token.getName(), authorizerName, name, null)
              );
              doFilter(filterChain, httpRequest, httpResponse);
            }
          } else {
            unauthorizedResponse = false;
          }
        }
        catch (AuthenticationException ex) {
          // exception from the filter itself is fatal
          errCode = HttpServletResponse.SC_FORBIDDEN;
          authenticationEx = ex;
          if (log.isDebugEnabled()) {
            log.debug(ex, "Authentication exception: " + ex.getMessage());
          } else {
            log.warn("Authentication exception: " + ex.getMessage());
          }
        }
        if (unauthorizedResponse) {
          if (!httpResponse.isCommitted()) {
            tokenToAuthCookie(
                httpResponse,
                "",
                getCookieDomain(),
                getCookiePath(),
                0,
                false,
                isHttps
            );
            // If response code is 401. Then WWW-Authenticate Header should be
            // present.. reset to 403 if not found..
            if ((errCode == HttpServletResponse.SC_UNAUTHORIZED)
                && (!httpResponse.containsHeader(
                org.apache.hadoop.security.authentication.client.KerberosAuthenticator.WWW_AUTHENTICATE))) {
              errCode = HttpServletResponse.SC_FORBIDDEN;
            }
            if (authenticationEx == null) {
              // Don't send an error response here, unlike the base AuthenticationFilter implementation.
              // This request did not use Kerberos auth.
              // Instead, we will send an error response in PreResponseAuthorizationCheckFilter to allow
              // other Authenticator implementations to check the request.
              filterChain.doFilter(request, response);
            } else {
              // Do send an error response here, we attempted Kerberos authentication and failed.
              httpResponse.sendError(errCode, authenticationEx.getMessage());
            }
          }
        }
      }
    };
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    Map<String, String> params = new HashMap<String, String>();
    params.put("kerberos.principal", serverPrincipal);
    params.put("kerberos.keytab", serverKeytab);
    params.put(AuthenticationFilter.AUTH_TYPE, DruidKerberosAuthenticationHandler.class.getName());
    params.put("kerberos.name.rules", authToLocal);
    if (cookieSignatureSecret != null) {
      params.put("signature.secret", cookieSignatureSecret);
    }
    return params;
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  @Override
  public String getAuthChallengeHeader()
  {
    return "Negotiate";
  }

  @Override
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    throw new UnsupportedOperationException("JDBC Kerberos auth not supported yet");
  }

  @Override
  public void decorateProxyRequest(
      HttpServletRequest clientRequest,
      HttpServletResponse proxyResponse,
      Request proxyRequest
  )
  {
    Object cookieToken = clientRequest.getAttribute(SIGNED_TOKEN_ATTRIBUTE);
    if (cookieToken != null && cookieToken instanceof String) {
      log.debug("Found cookie token will attache it to proxyRequest as cookie");
      String authResult = (String) cookieToken;
      String existingCookies = proxyRequest.getCookies()
                                           .stream()
                                           .map(HttpCookie::toString)
                                           .collect(Collectors.joining(";"));
      proxyRequest.header(HttpHeader.COOKIE, Joiner.on(";").join(authResult, existingCookies));
    }
  }

  /**
   * Kerberos context configuration for the JDK GSS library. Copied from hadoop-auth's KerberosAuthenticationHandler.
   */
  public static class DruidKerberosConfiguration extends Configuration
  {
    private String keytab;
    private String principal;

    public DruidKerberosConfiguration(String keytab, String principal)
    {
      this.keytab = keytab;
      this.principal = principal;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name)
    {
      Map<String, String> options = new HashMap<String, String>();
      if (System.getProperty("java.vendor").contains("IBM")) {
        options.put(
            "useKeytab",
            keytab.startsWith("file://") ? keytab : "file://" + keytab
        );
        options.put("principal", principal);
        options.put("credsType", "acceptor");
      } else {
        options.put("keyTab", keytab);
        options.put("principal", principal);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("useTicketCache", "true");
        options.put("renewTGT", "true");
        options.put("isInitiator", "false");
      }
      options.put("refreshKrb5Config", "true");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        if (System.getProperty("java.vendor").contains("IBM")) {
          options.put("useDefaultCcache", "true");
          // The first value searched when "useDefaultCcache" is used.
          System.setProperty("KRB5CCNAME", ticketCache);
          options.put("renewTGT", "true");
          options.put("credsType", "both");
        } else {
          options.put("ticketCache", ticketCache);
        }
      }
      if (log.isDebugEnabled()) {
        options.put("debug", "true");
      }

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(
              KerberosUtil.getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options
          ),
          };
    }
  }

  private void initializeKerberosLogin() throws ServletException
  {
    String keytab;

    try {
      if (serverPrincipal == null || serverPrincipal.trim().length() == 0) {
        throw new ServletException("Principal not defined in configuration");
      }
      keytab = serverKeytab;
      if (keytab == null || keytab.trim().length() == 0) {
        throw new ServletException("Keytab not defined in configuration");
      }
      if (!new File(keytab).exists()) {
        throw new ServletException("Keytab does not exist: " + keytab);
      }

      Set<Principal> principals = new HashSet<Principal>();
      principals.add(new KerberosPrincipal(serverPrincipal));
      Subject subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());

      DruidKerberosConfiguration kerberosConfiguration = new DruidKerberosConfiguration(keytab, serverPrincipal);

      log.info("Login using keytab " + keytab + ", for principal " + serverPrincipal);
      loginContext = new LoginContext("", subject, null, kerberosConfiguration);
      loginContext.login();

      log.info("Initialized, principal %s from keytab %s", serverPrincipal, keytab);
    }
    catch (Exception ex) {
      throw new ServletException(ex);
    }
  }


  /**
   * Creates the Hadoop authentication HTTP cookie.
   *
   * @param resp the response object.
   * @param token authentication token for the cookie.
   * @param domain the cookie domain.
   * @param path the cookie path.
   * @param expires UNIX timestamp that indicates the expire date of the
   *                cookie. It has no effect if its value &lt; 0.
   * @param isSecure is the cookie secure?
   * @param isCookiePersistent whether the cookie is persistent or not.
   *the following code copy/past from Hadoop 3.0.0 copied to avoid compilation issue due to new signature,
   *                           org.apache.hadoop.security.authentication.server.AuthenticationFilter#createAuthCookie
   *                           (
   *                           javax.servlet.http.HttpServletResponse,
   *                           java.lang.String,
   *                           java.lang.String,
   *                           java.lang.String,
   *                           long, boolean, boolean)
   */
  private static void tokenToAuthCookie(
      HttpServletResponse resp,
      String token,
      String domain,
      String path,
      long expires,
      boolean isCookiePersistent,
      boolean isSecure
  )
  {
    resp.addHeader("Set-Cookie", tokenToCookieString(token, domain, path, expires, isCookiePersistent, isSecure));
  }

  private static String tokenToCookieString(
      String token,
      String domain, String path, long expires,
      boolean isCookiePersistent,
      boolean isSecure
  )
  {
    StringBuilder sb = new StringBuilder(AuthenticatedURL.AUTH_COOKIE)
        .append("=");
    if (token != null && token.length() > 0) {
      sb.append("\"").append(token).append("\"");
    }

    if (path != null) {
      sb.append("; Path=").append(path);
    }

    if (domain != null) {
      sb.append("; Domain=").append(domain);
    }

    if (expires >= 0 && isCookiePersistent) {
      Date date = new Date(expires);
      SimpleDateFormat df = new SimpleDateFormat("EEE, dd-MMM-yyyy HH:mm:ss zzz", Locale.ENGLISH);
      df.setTimeZone(TimeZone.getTimeZone("GMT"));
      sb.append("; Expires=").append(df.format(date));
    }

    if (isSecure) {
      sb.append("; Secure");
    }

    sb.append("; HttpOnly");
    return sb.toString();
  }
}
