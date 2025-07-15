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

import org.apache.druid.error.DruidException;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.definition.CommonProfileDefinition;

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
    sessionStore.set(webContext1, "pac4jUserProfiles", profile);

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(900, cookie.getMaxAge());

    WebContext webContext2 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext2.getRequestCookies()).andReturn(Collections.singletonList(cookie));
    EasyMock.replay(webContext2);

    Optional<Object> value = sessionStore.get(webContext2, "pac4jUserProfiles");
    Assert.assertTrue(Objects.requireNonNull(value).isPresent());
    Assert.assertEquals("name", ((CommonProfile) value.get()).getAttribute(CommonProfileDefinition.DISPLAY_NAME));
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
    CommonProfile profile2 = new CommonProfile();
    profile2.setId("profile2");
    profile2.addAttribute(CommonProfileDefinition.DISPLAY_NAME, "name2");
    Map<String, CommonProfile> profiles = new HashMap<>();
    profiles.put("profile1", profile1);
    profiles.put("profile2", profile2);
    sessionStore.set(webContext1, "pac4jUserProfiles", profiles);

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertEquals(900, cookie.getMaxAge());

    WebContext webContext2 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext2.getRequestCookies()).andReturn(Collections.singletonList(cookie));
    EasyMock.replay(webContext2);

    Optional<Object> value = sessionStore.get(webContext2, "pac4jUserProfiles");
    Assert.assertTrue(Objects.requireNonNull(value).isPresent());
    Assert.assertEquals(2, ((Map<String, CommonProfile>) value.get()).size());
    EasyMock.verify(webContext2);
  }

  @Test
  public void testGetWithWrongPassphraseThrowsDruidException()
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
    DruidException exception = Assert.assertThrows(
            DruidException.class,
            () -> sessionStore.get(getContext, "key")
    );
    Assert.assertEquals(
            "Decryption failed. Check service logs.",
            exception.getMessage()
    );
    Assert.assertNotNull(exception.getCause());

    EasyMock.verify(getContext);
  }
}