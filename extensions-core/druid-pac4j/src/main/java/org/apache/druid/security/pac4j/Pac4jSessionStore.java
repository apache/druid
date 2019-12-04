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

import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.pac4j.core.context.ContextHelper;
import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.Pac4jConstants;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.exception.TechnicalException;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.util.JavaSerializationHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Code here is slight adaptation from <a href="https://github.com/apache/knox/blob/master/gateway-provider-security-pac4j/src/main/java/org/apache/knox/gateway/pac4j/session/KnoxSessionStore.java">KnoxSessionStore</a>
 * for storing oauth session information in cookies.
 */
public class Pac4jSessionStore implements SessionStore
{

  private static final Logger LOGGER = new Logger(Pac4jSessionStore.class);

  public static final String PAC4J_SESSION_PREFIX = "pac4j.session.";

  private final JavaSerializationHelper javaSerializationHelper;

  public Pac4jSessionStore()
  {
    javaSerializationHelper = new JavaSerializationHelper();
  }

  @Override
  public String getOrCreateSessionId(WebContext context)
  {
    return null;
  }

  private Serializable uncompressDecryptBase64(final String v)
  {
    if (v != null && !v.isEmpty()) {
      byte[] clear = StringUtils.decodeBase64String(v);
      if (clear != null) {
        try {
          return javaSerializationHelper.unserializeFromBytes(unCompress(clear));
        }
        catch (IOException e) {
          throw new TechnicalException(e);
        }
      }
    }
    return null;
  }

  @Override
  public Object get(WebContext context, String key)
  {
    final Cookie cookie = ContextHelper.getCookie(context, PAC4J_SESSION_PREFIX + key);
    Object value = null;
    if (cookie != null) {
      value = uncompressDecryptBase64(cookie.getValue());
    }
    LOGGER.debug("Get from session: [%s] = [%s]", key, value);
    return value;
  }

  private String compressEncryptBase64(final Object o)
  {
    if (o == null || "".equals(o)
        || (o instanceof Map<?, ?> && ((Map<?, ?>) o).isEmpty())) {
      return null;
    } else {
      byte[] bytes = javaSerializationHelper.serializeToBytes((Serializable) o);

      /* compress the data  */
      try {
        bytes = compress(bytes);

        if (bytes.length > 3000) {
          LOGGER.warn("Cookie too big, it might not be properly set");
        }

      }
      catch (final IOException e) {
        throw new TechnicalException(e);
      }

      return StringUtils.encodeBase64String(bytes);
    }
  }

  @Override
  public void set(WebContext context, String key, Object value)
  {
    Object profile = value;
    Cookie cookie;

    if (value == null) {
      cookie = new Cookie(PAC4J_SESSION_PREFIX + key, null);
    } else {
      if (key.contentEquals(Pac4jConstants.USER_PROFILES)) {
        /* trim the profile object */
        profile = clearUserProfile(value);
      }
      LOGGER.debug("Save in session: [%s] = [%s]", key, profile);
      cookie = new Cookie(
          PAC4J_SESSION_PREFIX + key,
          compressEncryptBase64(profile)
      );
    }

    cookie.setDomain("");
    cookie.setHttpOnly(true);
    cookie.setSecure(ContextHelper.isHttpsOrSecure(context));
    cookie.setPath("/");
    cookie.setMaxAge(900);

    context.addResponseCookie(cookie);
  }

  private static byte[] compress(final byte[] data) throws IOException
  {
    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(data.length)) {
      try (GZIPOutputStream gzip = new GZIPOutputStream(byteStream)) {
        gzip.write(data);
      }
      return byteStream.toByteArray();
    }
  }

  private static byte[] unCompress(final byte[] data) throws IOException
  {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
         GZIPInputStream gzip = new GZIPInputStream(inputStream)) {
      return IOUtils.toByteArray(gzip);
    }
  }

  private Object clearUserProfile(final Object value)
  {
    if (value instanceof Map<?, ?>) {
      final Map<String, CommonProfile> profiles = (Map<String, CommonProfile>) value;
      profiles.forEach((name, profile) -> profile.clearSensitiveData());
      return profiles;
    } else {
      final CommonProfile profile = (CommonProfile) value;
      profile.clearSensitiveData();
      return profile;
    }
  }

  @Override
  public SessionStore buildFromTrackableSession(WebContext arg0, Object arg1)
  {
    return null;
  }

  @Override
  public boolean destroySession(WebContext arg0)
  {
    return false;
  }

  @Override
  public Object getTrackableSession(WebContext arg0)
  {
    return null;
  }

  @Override
  public boolean renewSession(final WebContext context)
  {
    return false;
  }
}
