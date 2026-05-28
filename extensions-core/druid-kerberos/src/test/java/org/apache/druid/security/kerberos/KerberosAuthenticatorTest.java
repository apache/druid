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
import org.apache.druid.server.security.AuthConfig;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.http.HttpCookie;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

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


  /**
   * Verifies that an empty hadoop.auth cookie value is treated as "no cookie" rather than
   * causing a SignerException. An empty cookie results from a prior session expiry where
   * Druid cleared the cookie. Without this fix, the empty value would be passed to
   * Signer.verifyAndExtract("") which throws SignerException, setting authenticationEx
   * and causing the entire auth chain to short-circuit with a 403.
   */
  @Test
  public void testGetTokenWithEmptyCookieReturnsNull() throws Exception
  {
    final Filter filter = createFilterWithSigner();
    final Method getToken = findGetTokenMethod();

    // Empty cookie value - the real-world scenario after session expiry clears the cookie.
    // Without the fix, Signer.verifyAndExtract("") throws SignerException.
    final HttpServletRequest requestWithEmptyCookie = mockRequestWithEmptyCookie();
    final AuthenticationToken token = (AuthenticationToken) getToken.invoke(filter, requestWithEmptyCookie);
    Assert.assertNull("Empty hadoop.auth cookie should be treated as no cookie", token);

    // No cookie at all - baseline, should return null
    final HttpServletRequest requestWithNoCookie = Mockito.mock(HttpServletRequest.class);
    Mockito.when(requestWithNoCookie.getCookies()).thenReturn(null);
    final AuthenticationToken tokenForNoCookie = (AuthenticationToken) getToken.invoke(filter, requestWithNoCookie);
    Assert.assertNull("Missing hadoop.auth cookie should return null", tokenForNoCookie);
  }

  private Filter createFilterWithSigner() throws Exception
  {
    final Filter filter = new KerberosAuthenticator(
        TEST_SERVER_PRINCIPAL,
        TEST_SERVER_KEYTAB,
        TEST_AUTH_TO_LOCAL,
        TEST_COOKIE_SECRET,
        TEST_AUTHORIZER_NAME,
        TEST_NAME,
        createTestNode()
    ).getFilter();

    final SignerSecretProvider secretProvider = new SignerSecretProvider()
    {
      @Override
      public void init(Properties config, ServletContext servletContext, long tokenValidity)
      {
      }

      @Override
      public byte[] getCurrentSecret()
      {
        return TEST_COOKIE_SECRET.getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public byte[][] getAllSecrets()
      {
        return new byte[][]{TEST_COOKIE_SECRET.getBytes(StandardCharsets.UTF_8)};
      }
    };
    final Signer signer = new Signer(secretProvider);

    // Inject mySigner into the anonymous AuthenticationFilter subclass via reflection
    for (Field field : filter.getClass().getDeclaredFields()) {
      if (field.getType().equals(Signer.class)) {
        field.setAccessible(true);
        field.set(filter, signer);
        break;
      }
    }
    return filter;
  }

  private Method findGetTokenMethod() throws Exception
  {
    final Method method = AuthenticationFilter.class.getDeclaredMethod("getToken", HttpServletRequest.class);
    method.setAccessible(true);
    return method;
  }

  private HttpServletRequest mockRequestWithEmptyCookie()
  {
    final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    final Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, "");
    Mockito.when(request.getCookies()).thenReturn(new Cookie[]{cookie});
    return request;
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

  @Test
  public void testTokenToCookieStringWithZeroExpiresIncludesMaxAge() throws Exception
  {
    final Method method = KerberosAuthenticator.class.getDeclaredMethod(
        "tokenToCookieString",
        String.class,
        String.class,
        String.class,
        long.class,
        boolean.class,
        boolean.class
    );
    method.setAccessible(true);

    // Test case: expires = 0 (intended for cookie deletion)
    final String cookieString = (String) method.invoke(
        null,
        "",         // token
        "localhost", // domain
        "/",        // path
        0,          // expires
        false,      // isCookiePersistent
        false       // isSecure
    );

    Assert.assertTrue("Cookie string should contain 'Max-Age=0'", cookieString.contains("Max-Age=0"));
    Assert.assertFalse("Cookie string should not contain 'Expires=' when expires is 0", cookieString.contains("Expires="));

    // Test case: expires > 0 and persistent
    final String persistentCookieString = (String) method.invoke(
        null,
        "some-token",
        "localhost",
        "/",
        System.currentTimeMillis() + 3600,
        true,
        false
    );
    Assert.assertTrue("Persistent cookie should contain 'Expires='", persistentCookieString.contains("Expires="));
    Assert.assertFalse("Persistent cookie should not contain 'Max-Age=0'", persistentCookieString.contains("Max-Age=0"));
  }

  @Test
  public void testTokenToCookieStringWithNullToken() throws Exception
  {
    final Method method = KerberosAuthenticator.class.getDeclaredMethod(
        "tokenToCookieString",
        String.class,
        String.class,
        String.class,
        long.class,
        boolean.class,
        boolean.class
    );
    method.setAccessible(true);

    final String cookieString = (String) method.invoke(null, null, "localhost", "/", 0, false, false);

    Assert.assertFalse("Null token should not add quoted value", cookieString.contains("\""));
    Assert.assertTrue("Cookie string should contain Max-Age=0", cookieString.contains("Max-Age=0"));
  }

  @Test
  public void testTokenToCookieStringWithNonPersistentPositiveExpires() throws Exception
  {
    final Method method = KerberosAuthenticator.class.getDeclaredMethod(
        "tokenToCookieString",
        String.class,
        String.class,
        String.class,
        long.class,
        boolean.class,
        boolean.class
    );
    method.setAccessible(true);

    final String cookieString = (String) method.invoke(
        null,
        "some-token",
        "localhost",
        "/",
        System.currentTimeMillis() + 3600,
        false,  // isCookiePersistent = false
        false
    );

    Assert.assertFalse("Non-persistent cookie should not contain 'Expires='", cookieString.contains("Expires="));
    Assert.assertFalse("Non-persistent cookie should not contain 'Max-Age=0'", cookieString.contains("Max-Age=0"));
  }

  @Test
  public void testDecorateProxyRequestWithStringToken()
  {
    final KerberosAuthenticator authenticator = new KerberosAuthenticator(
        TEST_SERVER_PRINCIPAL,
        TEST_SERVER_KEYTAB,
        TEST_AUTH_TO_LOCAL,
        TEST_COOKIE_SECRET,
        TEST_AUTHORIZER_NAME,
        TEST_NAME,
        createTestNode()
    );
    final HttpServletRequest clientRequest = Mockito.mock(HttpServletRequest.class);
    final HttpServletResponse proxyResponse = Mockito.mock(HttpServletResponse.class);
    final Request proxyRequest = Mockito.mock(Request.class);
    Mockito.when(clientRequest.getAttribute(KerberosAuthenticator.SIGNED_TOKEN_ATTRIBUTE)).thenReturn("signed-token-value");

    authenticator.decorateProxyRequest(clientRequest, proxyResponse, proxyRequest);

    Mockito.verify(proxyRequest).cookie(ArgumentMatchers.any(HttpCookie.class));
  }

  @Test
  public void testDecorateProxyRequestWithoutToken()
  {
    final KerberosAuthenticator authenticator = new KerberosAuthenticator(
        TEST_SERVER_PRINCIPAL,
        TEST_SERVER_KEYTAB,
        TEST_AUTH_TO_LOCAL,
        TEST_COOKIE_SECRET,
        TEST_AUTHORIZER_NAME,
        TEST_NAME,
        createTestNode()
    );
    final HttpServletRequest clientRequest = Mockito.mock(HttpServletRequest.class);
    final HttpServletResponse proxyResponse = Mockito.mock(HttpServletResponse.class);
    final Request proxyRequest = Mockito.mock(Request.class);
    Mockito.when(clientRequest.getAttribute(KerberosAuthenticator.SIGNED_TOKEN_ATTRIBUTE)).thenReturn(null);

    authenticator.decorateProxyRequest(clientRequest, proxyResponse, proxyRequest);

    Mockito.verify(proxyRequest, Mockito.never()).cookie(ArgumentMatchers.any());
  }

  @Test
  public void testDoFilterWithNullPrincipalThrowsServletException()
  {
    // null serverPrincipal causes initializeKerberosLogin to throw "Principal not defined"
    final KerberosAuthenticator authenticator = new KerberosAuthenticator(
        null,
        TEST_SERVER_KEYTAB,
        TEST_AUTH_TO_LOCAL,
        TEST_COOKIE_SECRET,
        TEST_AUTHORIZER_NAME,
        TEST_NAME,
        createTestNode()
    );
    final Filter filter = authenticator.getFilter();
    final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    final FilterChain chain = Mockito.mock(FilterChain.class);
    Mockito.when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(null);

    final ServletException ex = Assert.assertThrows(
        ServletException.class,
        () -> filter.doFilter(request, response, chain)
    );
    Assert.assertTrue(ex.getMessage().contains("Principal not defined"));
  }

  @Test
  public void testDoFilterWithNullKeytabThrowsServletException()
  {
    final KerberosAuthenticator authenticator = new KerberosAuthenticator(
        TEST_SERVER_PRINCIPAL,
        null,  // null keytab
        TEST_AUTH_TO_LOCAL,
        TEST_COOKIE_SECRET,
        TEST_AUTHORIZER_NAME,
        TEST_NAME,
        createTestNode()
    );
    final Filter filter = authenticator.getFilter();
    final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    final FilterChain chain = Mockito.mock(FilterChain.class);
    Mockito.when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(null);

    final ServletException ex = Assert.assertThrows(
        ServletException.class,
        () -> filter.doFilter(request, response, chain)
    );
    Assert.assertTrue(ex.getMessage().contains("Keytab not defined"));
  }

  @Test
  public void testDoFilterWithEmptyKeytabThrowsServletException()
  {
    final KerberosAuthenticator authenticator = new KerberosAuthenticator(
        TEST_SERVER_PRINCIPAL,
        "",  // empty keytab
        TEST_AUTH_TO_LOCAL,
        TEST_COOKIE_SECRET,
        TEST_AUTHORIZER_NAME,
        TEST_NAME,
        createTestNode()
    );
    final Filter filter = authenticator.getFilter();
    final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    final FilterChain chain = Mockito.mock(FilterChain.class);
    Mockito.when(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).thenReturn(null);

    final ServletException ex = Assert.assertThrows(
        ServletException.class,
        () -> filter.doFilter(request, response, chain)
    );
    Assert.assertTrue(ex.getMessage().contains("Keytab not defined"));
  }
}
