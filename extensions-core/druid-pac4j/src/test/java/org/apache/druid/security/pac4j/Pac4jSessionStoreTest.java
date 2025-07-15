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

package org.apache.druid.security.pac4j;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.definition.CommonProfileDefinition;
import org.pac4j.core.util.Pac4jConstants;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class Pac4jSessionStoreTest
{
  private static final String COOKIE_PASSPHRASE = "test-cookie-passphrase";

  @Test
  public void testSetAndGet()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext1 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext1.getScheme()).andReturn("https");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext1.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext1);

    sessionStore.set(webContext1, "key", "value");

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(900, cookie.getMaxAge());

    // For the get test, we need to mock the context to return the cookie
    WebContext webContext2 = EasyMock.mock(WebContext.class);
    // The get method will call getRequestCookies() once for each getCookie() call
    EasyMock.expect(webContext2.getRequestCookies()).andReturn(Collections.singletonList(cookie));
    EasyMock.replay(webContext2);

    Assert.assertEquals("value", Objects.requireNonNull(sessionStore.get(webContext2, "key")).orElse(null));
    EasyMock.verify(webContext2);
  }

  @Test
  public void testSetAndGetWithHttpScheme()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext1 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext1.getScheme()).andReturn("http");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext1.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext1);

    sessionStore.set(webContext1, "key", "value");

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure()); // Should still be secure due to our fix
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(900, cookie.getMaxAge());

    EasyMock.verify(webContext1);
  }

  @Test
  public void testSetNullValue()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext.getScheme()).andReturn("https");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext);

    sessionStore.set(webContext, "key", null);

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(0, cookie.getMaxAge()); // Should be 0 for null values
    Assert.assertEquals("", cookie.getValue());

    EasyMock.verify(webContext);
  }

  @Test
  public void testSetEmptyString()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext.getScheme()).andReturn("https");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext);

    sessionStore.set(webContext, "key", "");

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(0, cookie.getMaxAge()); // Should be 0 for empty string
    Assert.assertEquals("", cookie.getValue());

    EasyMock.verify(webContext);
  }

  @Test
  public void testSetEmptyMap()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext.getScheme()).andReturn("https");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext);

    sessionStore.set(webContext, "key", Collections.emptyMap());

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(0, cookie.getMaxAge()); // Should be 0 for empty map
    Assert.assertEquals("", cookie.getValue());

    EasyMock.verify(webContext);
  }

  @Test
  public void testGetWithNoCookies()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext.getRequestCookies()).andReturn(Collections.emptyList());
    EasyMock.replay(webContext);

    Optional<Object> result = sessionStore.get(webContext, "key");
    Assert.assertFalse(result.isPresent());

    EasyMock.verify(webContext);
  }

  @Test
  public void testGetWithNullCookies()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext.getRequestCookies()).andReturn(null);
    EasyMock.replay(webContext);

    Optional<Object> result = sessionStore.get(webContext, "key");
    Assert.assertFalse(result.isPresent());

    EasyMock.verify(webContext);
  }

  @Test
  public void testGetWithNullValue()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext = EasyMock.mock(WebContext.class);
    Cookie cookie = EasyMock.mock(Cookie.class);
    EasyMock.expect(cookie.getName()).andReturn("pac4j.session.key");
    EasyMock.expect(cookie.getValue()).andReturn(null);
    EasyMock.expect(webContext.getRequestCookies()).andReturn(Collections.singletonList(cookie));
    EasyMock.replay(webContext, cookie);

    Optional<Object> result = sessionStore.get(webContext, "key");
    Assert.assertFalse(result.isPresent());

    EasyMock.verify(webContext, cookie);
  }

  @Test
  public void testGetWithEmptyValue()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext = EasyMock.mock(WebContext.class);
    Cookie cookie = EasyMock.mock(Cookie.class);
    EasyMock.expect(cookie.getName()).andReturn("pac4j.session.key");
    EasyMock.expect(cookie.getValue()).andReturn("");
    EasyMock.expect(webContext.getRequestCookies()).andReturn(Collections.singletonList(cookie));
    EasyMock.replay(webContext, cookie);

    Optional<Object> result = sessionStore.get(webContext, "key");
    Assert.assertFalse(result.isPresent());

    EasyMock.verify(webContext, cookie);
  }

  @Test
  public void testSetAndGetClearUserProfile()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext1 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext1.getScheme()).andReturn("https");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext1.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext1);

    CommonProfile profile = new CommonProfile();
    profile.setId("profile1");
    profile.addAttribute(CommonProfileDefinition.DISPLAY_NAME, "name");
    profile.addAttribute("access_token", "token");
    profile.addAttribute("refresh_token", "refresh");
    profile.addAttribute("id_token", "id");
    profile.addAttribute("credentials", "creds");
    
    sessionStore.set(webContext1, Pac4jConstants.USER_PROFILES, profile);

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(900, cookie.getMaxAge());

    WebContext webContext2 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext2.getRequestCookies()).andReturn(Collections.singletonList(cookie));
    EasyMock.replay(webContext2);

    Optional<Object> value = sessionStore.get(webContext2, Pac4jConstants.USER_PROFILES);
    Assert.assertTrue(Objects.requireNonNull(value).isPresent());
    CommonProfile retrievedProfile = (CommonProfile) value.get();
    Assert.assertEquals("name", retrievedProfile.getAttribute(CommonProfileDefinition.DISPLAY_NAME));
    
    // Verify sensitive data was removed
    Assert.assertNull(retrievedProfile.getAttribute("access_token"));
    Assert.assertNull(retrievedProfile.getAttribute("refresh_token"));
    Assert.assertNull(retrievedProfile.getAttribute("id_token"));
    Assert.assertNull(retrievedProfile.getAttribute("credentials"));
    
    EasyMock.verify(webContext2);
  }

  @Test
  public void testSetAndGetClearUserMultipleProfile()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext1 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext1.getScheme()).andReturn("https");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext1.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext1);

    CommonProfile profile1 = new CommonProfile();
    profile1.setId("profile1");
    profile1.addAttribute(CommonProfileDefinition.DISPLAY_NAME, "name1");
    profile1.addAttribute("access_token", "token1");
    
    CommonProfile profile2 = new CommonProfile();
    profile2.setId("profile2");
    profile2.addAttribute(CommonProfileDefinition.DISPLAY_NAME, "name2");
    profile2.addAttribute("refresh_token", "refresh2");
    
    Map<String, CommonProfile> profiles = new HashMap<>();
    profiles.put("profile1", profile1);
    profiles.put("profile2", profile2);
    
    sessionStore.set(webContext1, Pac4jConstants.USER_PROFILES, profiles);

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(900, cookie.getMaxAge());

    WebContext webContext2 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext2.getRequestCookies()).andReturn(Collections.singletonList(cookie));
    EasyMock.replay(webContext2);

    Optional<Object> value = sessionStore.get(webContext2, Pac4jConstants.USER_PROFILES);
    Assert.assertTrue(Objects.requireNonNull(value).isPresent());
    @SuppressWarnings("unchecked")
    Map<String, CommonProfile> retrievedProfiles = (Map<String, CommonProfile>) value.get();
    Assert.assertEquals(2, retrievedProfiles.size());
    
    // Verify sensitive data was removed from both profiles
    Assert.assertNull(retrievedProfiles.get("profile1").getAttribute("access_token"));
    Assert.assertNull(retrievedProfiles.get("profile2").getAttribute("refresh_token"));
    
    EasyMock.verify(webContext2);
  }

  @Test
  public void testSessionStoreInterfaceMethods()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);
    WebContext webContext = EasyMock.mock(WebContext.class);
    EasyMock.replay(webContext);

    // Test methods that return empty/false for non-JEE contexts
    Assert.assertFalse(sessionStore.getSessionId(webContext, true).isPresent());
    Assert.assertFalse(sessionStore.destroySession(webContext));
    Assert.assertFalse(sessionStore.getTrackableSession(webContext).isPresent());
    Assert.assertFalse(sessionStore.buildFromTrackableSession(webContext, "test").isPresent());
    Assert.assertFalse(sessionStore.renewSession(webContext));

    EasyMock.verify(webContext);
  }

  @Test
  public void testGetWithWrongPassphraseThrowsException()
  {
    final WebContext webContext = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext.getScheme()).andReturn("https");

    final Capture<Cookie> cookieCapture = EasyMock.newCapture();
    webContext.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.expectLastCall().once();
    EasyMock.replay(webContext);

    // Create a cookie with an invalid passphrase
    new Pac4jSessionStore("invalid-passphrase").set(webContext, "key", "value");

    EasyMock.verify(webContext);

    // Create a new mock for the get operation
    final WebContext getContext = EasyMock.mock(WebContext.class);

    // The captured cookie should have the correct name "pac4j.session.key"
    Cookie capturedCookie = cookieCapture.getValue();
    EasyMock.expect(getContext.getRequestCookies())
            .andReturn(Collections.singletonList(capturedCookie));
    EasyMock.replay(getContext);

    // Verify that trying to decrypt the invalid cookie throws an exception
    final Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);
    RuntimeException exception = Assert.assertThrows(
        RuntimeException.class,
        () -> sessionStore.get(getContext, "key")
    );
    Assert.assertTrue(exception.getMessage().contains("Decryption failed"));
    Assert.assertNotNull(exception.getCause());

    EasyMock.verify(getContext);
  }

  @Test
  public void testLargeCookieWarning()
  {
    Pac4jSessionStore sessionStore = new Pac4jSessionStore(COOKIE_PASSPHRASE);

    WebContext webContext = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext.getScheme()).andReturn("https");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext);

    // Create a large object that will result in a big cookie
    StringBuilder largeData = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      largeData.append("This is a large piece of data that will make the cookie very big. ");
    }

    sessionStore.set(webContext, "key", largeData.toString());

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(900, cookie.getMaxAge());

    EasyMock.verify(webContext);
  }
}

