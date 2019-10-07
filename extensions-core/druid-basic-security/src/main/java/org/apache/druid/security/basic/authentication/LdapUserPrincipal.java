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

package org.apache.druid.security.basic.authentication;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;

import javax.naming.directory.SearchResult;
import java.security.Principal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class LdapUserPrincipal implements Principal
{
  private static final Logger LOG = new Logger(LdapUserPrincipal.class);

  private final String name;
  private final BasicAuthenticatorCredentials credentials;
  private final SearchResult searchResult;
  private final Instant createdAt;
  private final AtomicReference<Instant> lastVerified = new AtomicReference<>();

  public LdapUserPrincipal(
      String name,
      BasicAuthenticatorCredentials credentials,
      SearchResult searchResult
  )
  {
    this(name, credentials, searchResult, Instant.now());
  }

  private LdapUserPrincipal(
      String name,
      BasicAuthenticatorCredentials credentials,
      SearchResult searchResult,
      Instant createdAt
  )
  {
    Objects.requireNonNull(name, "name is required");
    Objects.requireNonNull(credentials, "credentials is required");
    Objects.requireNonNull(searchResult, "searchResult is required");
    Objects.requireNonNull(createdAt, "createdAt is required");

    this.name = name;
    this.credentials = credentials;
    this.searchResult = searchResult;
    this.createdAt = createdAt;
    this.lastVerified.set(createdAt);
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  public SearchResult getSearchResult()
  {
    return searchResult;
  }

  public Instant getCreatedAt()
  {
    return createdAt;
  }

  public Instant getLastVerified()
  {
    return lastVerified.get();
  }

  public boolean hasSameCredentials(char[] password)
  {
    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        password,
        this.credentials.getSalt(),
        this.credentials.getIterations()
    );
    if (Arrays.equals(recalculatedHash, credentials.getHash())) {
      this.lastVerified.set(Instant.now());
      LOG.debug("Refereshing lastVerified principal user '%s'", this.name);
      return true;
    } else {
      return false;
    }
  }

  public boolean isExpired(int duration, int maxDuration)
  {
    long now = System.currentTimeMillis();

    long maxCutoff = now - (maxDuration * 1000L);
    if (this.createdAt.toEpochMilli() < maxCutoff) {
      long cutoff = now - (duration * 1000L);
      if (this.lastVerified.get().toEpochMilli() < cutoff) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public String toString()
  {
    return StringUtils.format(
        "LdapUserPrincipal[name=%s, searchResult=%s, createdAt=%s, lastVerified=%s]",
        name,
        searchResult,
        createdAt,
        lastVerified);
  }
}
