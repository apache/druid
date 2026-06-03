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

package org.apache.druid.common.config;

import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

/**
 * Computes and compares ETags for stored dynamic config payloads. The ETag is a
 * pure function of the payload bytes — used for {@code If-Match} preconditions.
 */
public final class ConfigEtag
{
  private static final int ETAG_HASH_BYTES = 16;
  private static final ThreadLocal<MessageDigest> SHA_256 = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-256");
    }
    catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  });

  private ConfigEtag()
  {
  }

  /**
   * Quoted ETag for the given payload bytes, or {@code null} if {@code bytes}
   * is {@code null}. SHA-256 truncated to {@value #ETAG_HASH_BYTES} bytes,
   * base64url-encoded (no padding), wrapped in double quotes per RFC 7232.
   */
  @Nullable
  public static String compute(@Nullable byte[] bytes)
  {
    if (bytes == null) {
      return null;
    }
    final MessageDigest md = SHA_256.get();
    md.reset();
    final byte[] full = md.digest(bytes);
    final byte[] truncated = Arrays.copyOf(full, ETAG_HASH_BYTES);
    return "\"" + Base64.getUrlEncoder().withoutPadding().encodeToString(truncated) + "\"";
  }

  /**
   * Whether {@code ifMatchHeader} matches the ETag of {@code currentBytes}.
   * Wildcard {@code *} matches any existing value. A comma-separated list is
   * satisfied if any element matches.
   */
  public static boolean matches(@Nullable String ifMatchHeader, @Nullable byte[] currentBytes)
  {
    if (ifMatchHeader == null) {
      return true;
    }
    final String trimmed = ifMatchHeader.trim();
    if ("*".equals(trimmed)) {
      return currentBytes != null;
    }
    final String currentEtag = compute(currentBytes);
    if (currentEtag == null) {
      return false;
    }
    for (String candidate : trimmed.split(",")) {
      if (currentEtag.equals(candidate.trim())) {
        return true;
      }
    }
    return false;
  }
}
