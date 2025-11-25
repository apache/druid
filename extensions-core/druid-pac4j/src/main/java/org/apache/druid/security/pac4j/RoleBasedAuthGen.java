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

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import org.apache.druid.java.util.common.logger.Logger;
import org.pac4j.core.authorization.generator.AuthorizationGenerator;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.oidc.profile.OidcProfile;

import javax.validation.constraints.NotBlank;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class RoleBasedAuthGen implements AuthorizationGenerator
{

  private static final Logger LOG = new Logger(RoleBasedAuthGen.class);

  private final String roleClaimPath; // dot separated path to roles claim in the ID token

  public RoleBasedAuthGen(@NotBlank String roleClaimPath)
  {
    this.roleClaimPath = roleClaimPath;
  }

  @Override
  public Optional<UserProfile> generate(WebContext context, SessionStore sessionStore, UserProfile profile)
  {
    if (profile == null) {
      return Optional.empty();
    }

    if (!(profile instanceof OidcProfile)) {
      return Optional.of(profile);
    }

    final AccessToken accessToken = ((OidcProfile) profile).getAccessToken();

    if (accessToken == null) {
      LOG.debug("No access token; skip role extraction");
      return Optional.of(profile);
    }

    final String tokenValue = accessToken.getValue();
    if (tokenValue == null || tokenValue.isBlank()) {
      LOG.debug("Empty access token, skip role extraction");
      return Optional.of(profile);
    }

    try {
      final JWT jwt = JWTParser.parse(tokenValue);
      JWTClaimsSet set = jwt.getJWTClaimsSet();
      if (set != null) {
        Map<String, Object> claims = set.getClaims();
        Set<String> roles = claimAtPath(claims, roleClaimPath);
        ((OidcProfile) profile).setRoles(roles);
        LOG.debug(
            "Extracted %,d roles from claim path [%s]: %s",
            roles.size(),
            roleClaimPath,
            roles
        );
      }
    }
    catch (Throwable t) {
      LOG.debug("No usable ID token on profile; skip extraction");
    }

    return Optional.of(profile);
  }


  private static Set<String> claimAtPath(final Map<String, Object> root, final String path)
  {
    if (root == null) {
      LOG.warn("No claims found in token");
      return Collections.emptySet();
    }

    final String trimmed = path.trim();
    if (trimmed.isEmpty()) {
      LOG.warn("Empty roles claim path");
      return Collections.emptySet();
    }

    Object cur = root;
    final String[] parts = trimmed.split("\\.");
    for (String key : parts) {
      if (!(cur instanceof Map)) {
        return Collections.emptySet();
      }
      final Map<String, Object> m = (Map<String, Object>) cur;
      if (!m.containsKey(key)) {
        return Collections.emptySet();
      }
      cur = m.get(key);
      if (cur == null) {
        return Collections.emptySet();
      }
    }
    return normalizeClaimValues(cur);
  }


  private static Set<String> normalizeClaimValues(Object claim)
  {
    if (claim == null) {
      return Set.of();
    }

    Stream<?> stream;
    if (claim instanceof Collection<?>) {
      stream = ((Collection<?>) claim).stream();
    } else if (claim.getClass().isArray()) {
      int len = Array.getLength(claim);
      stream = IntStream.range(0, len).mapToObj(i -> Array.get(claim, i));
    } else {
      stream = Stream.of(claim);
    }

    return stream
        .filter(Objects::nonNull)
        .map(o -> o.toString().trim())
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
  }
}
