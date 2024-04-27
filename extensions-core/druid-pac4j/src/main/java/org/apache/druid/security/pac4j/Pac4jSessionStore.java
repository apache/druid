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
import org.apache.druid.crypto.CryptoService;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.pac4j.core.context.ContextHelper;
import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.exception.TechnicalException;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.util.JavaSerializationHelper;
import org.pac4j.core.util.Pac4jConstants;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Code here is slight adaptation from <a href="https://github.com/apache/knox/blob/master/gateway-provider-security-pac4j/src/main/java/org/apache/knox/gateway/pac4j/session/KnoxSessionStore.java">KnoxSessionStore</a>
 * for storing oauth session information in cookies.
 */
public class Pac4jSessionStore<T extends WebContext> implements SessionStore<T>
{

  private static final Logger LOGGER = new Logger(Pac4jSessionStore.class);

  public static final String PAC4J_SESSION_PREFIX = "pac4j.session.";

  private final JavaSerializationHelper javaSerializationHelper;
  private final CryptoService cryptoService;

  public Pac4jSessionStore(String cookiePassphrase)
  {
    javaSerializationHelper = new JavaSerializationHelper();
    cryptoService = new CryptoService(
        cookiePassphrase,
        "AES",
        "CBC",
        "PKCS5Padding",
        "PBKDF2WithHmacSHA256",
        128,
        65536,
        128
    );
  }

  @Override
  public String getOrCreateSessionId(WebContext context)
  {
    return null;
  }

  @Nullable
  @Override
  public Optional<Object> get(WebContext context, String key)
  {
    final Cookie cookie = ContextHelper.getCookie(context, PAC4J_SESSION_PREFIX + key);
    Object value = null;
    if (cookie != null) {
      value = uncompressDecryptBase64(cookie.getValue());
    }
    LOGGER.debug("Get from session: [%s] = [%s]", key, value);
    return Optional.ofNullable(value);
  }

  @Override
  public void set(WebContext context, String key, @Nullable Object value)
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

  @Nullable
  private String compressEncryptBase64(final Object o)
  {
    if (o == null || "".equals(o)
        || (o instanceof Map<?, ?> && ((Map<?, ?>) o).isEmpty())) {
      return null;
    } else {
      byte[] bytes = javaSerializationHelper.serializeToBytes((Serializable) o);

      bytes = compress(bytes);
      if (bytes.length > 3000) {
        LOGGER.warn("Cookie too big, it might not be properly set");
      }

      return StringUtils.encodeBase64String(cryptoService.encrypt(bytes));
    }
  }

  @Nullable
  private Serializable uncompressDecryptBase64(final String v)
  {
    if (v != null && !v.isEmpty()) {
      byte[] bytes = StringUtils.decodeBase64String(v);
      if (bytes != null) {
        return javaSerializationHelper.deserializeFromBytes(unCompress(cryptoService.decrypt(bytes)));
      }
    }
    return null;
  }

  private byte[] compress(final byte[] data)
  {
    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(data.length)) {
      try (GZIPOutputStream gzip = new GZIPOutputStream(byteStream)) {
        gzip.write(data);
      }
      return byteStream.toByteArray();
    }
    catch (IOException ex) {
      throw new TechnicalException(ex);
    }
  }

  private byte[] unCompress(final byte[] data)
  {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
         GZIPInputStream gzip = new GZIPInputStream(inputStream)) {
      return IOUtils.toByteArray(gzip);
    }
    catch (IOException ex) {
      throw new TechnicalException(ex);
    }
  }

  private Object clearUserProfile(final Object value)
  {
    if (value instanceof Map<?, ?>) {
      final Map<String, CommonProfile> profiles = (Map<String, CommonProfile>) value;
      profiles.forEach((name, profile) -> profile.removeLoginData());
      return profiles;
    } else {
      final CommonProfile profile = (CommonProfile) value;
      profile.removeLoginData();
      return profile;
    }
  }

  @Override
  public Optional<SessionStore<T>> buildFromTrackableSession(WebContext arg0, Object arg1)
  {
    return Optional.empty();
  }

  @Override
  public boolean destroySession(WebContext arg0)
  {
    return false;
  }

  @Override
  public Optional getTrackableSession(WebContext arg0)
  {
    return Optional.empty();
  }

  @Override
  public boolean renewSession(final WebContext context)
  {
    return false;
  }
}
