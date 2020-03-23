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

import java.util.Collections;

public class Pac4jSessionStoreTest
{
  @Test
  public void testSetAndGet()
  {
    Pac4jSessionStore<WebContext> sessionStore = new Pac4jSessionStore("test-cookie-passphrase");

    WebContext webContext1 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext1.getScheme()).andReturn("https");
    Capture<Cookie> cookieCapture = EasyMock.newCapture();

    webContext1.addResponseCookie(EasyMock.capture(cookieCapture));
    EasyMock.replay(webContext1);

    sessionStore.set(webContext1, "key", "value");

    Cookie cookie = cookieCapture.getValue();
    Assert.assertTrue(cookie.isSecure());
    Assert.assertTrue(cookie.isHttpOnly());
    Assert.assertTrue(cookie.isSecure());
    Assert.assertEquals(900, cookie.getMaxAge());


    WebContext webContext2 = EasyMock.mock(WebContext.class);
    EasyMock.expect(webContext2.getRequestCookies()).andReturn(Collections.singletonList(cookie));
    EasyMock.replay(webContext2);

    Assert.assertEquals("value", sessionStore.get(webContext2, "key"));
  }
}
