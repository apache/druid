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
    Pac4jSessionStore sessionStore = new Pac4jSessionStore("test-cookie-passphrase");

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
