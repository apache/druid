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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Provider;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.auth.Credentials;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.security.basic.BasicAuthDBConfig;
import org.apache.druid.security.basic.escalator.db.cache.BasicEscalatorCacheManager;
import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;

import java.util.concurrent.atomic.AtomicReference;

@JsonTypeName("basic")
public class BasicHTTPEscalator implements Escalator
{
  private static final Logger LOG = new Logger(BasicHTTPEscalator.class);
  public static final int DEFAULT_INTERNAL_CLIENT_CREDENTIAL_POLL_SECONDS = 10;

  private final Provider<BasicEscalatorCacheManager> cacheManager;
  private final String authorizerName;
  private final BasicAuthDBConfig dbConfig;
  private final AtomicReference<BasicEscalatorCredential> cachedEscalatorCredential;
  private final AtomicReference<Long> lastVerified;
  private final int internalClientCredentialPoll;

  @JsonCreator
  public BasicHTTPEscalator(
      @JacksonInject Provider<BasicEscalatorCacheManager> cacheManager,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("internalClientUsername") String internalClientUsername,
      @JsonProperty("internalClientPassword") PasswordProvider internalClientPassword,
      @JsonProperty("enableCacheNotifications") Boolean enableCacheNotifications,
      @JsonProperty("cacheNotificationTimeout") Long cacheNotificationTimeout,
      @JsonProperty("internalClientCredentialPoll") Integer internalClientCredentialPoll
  )
  {
    this.cacheManager = cacheManager;
    this.authorizerName = authorizerName;
    this.cachedEscalatorCredential = new AtomicReference<>(
        new BasicEscalatorCredential(internalClientUsername, internalClientPassword.getPassword())
    );
    this.dbConfig = new BasicAuthDBConfig(
        null,
        null,
        null,
        null,
        null,
        enableCacheNotifications == null ? true : enableCacheNotifications,
        cacheNotificationTimeout == null ? BasicAuthDBConfig.DEFAULT_CACHE_NOTIFY_TIMEOUT_MS : cacheNotificationTimeout,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        null, null,
        null,
        null
    );
    this.internalClientCredentialPoll = internalClientCredentialPoll != null ? internalClientCredentialPoll : DEFAULT_INTERNAL_CLIENT_CREDENTIAL_POLL_SECONDS;
    Long now = System.currentTimeMillis();
    lastVerified = new AtomicReference<>(now);
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    LOG.debug("----------- Creating escalated client");
    return new CredentialedHttpClient(new BasicEscalatorCredentials(), baseClient);
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    BasicEscalatorCredential basicEscalatorCredential = getOrUpdateEscalatorCredentials();
    LOG.debug("----------- Creating escalated authentication result. username: %s", basicEscalatorCredential.getUsername());
    // if you found your self asking why the authenticatedBy field is set to null please read this:
    // https://github.com/apache/incubator-druid/pull/5706#discussion_r185940889
    return new AuthenticationResult(
        basicEscalatorCredential.getUsername(),
        authorizerName,
        null,
        null);
  }

  public BasicAuthDBConfig getDbConfig()
  {
    return dbConfig;
  }
  
  private final class BasicEscalatorCredentials implements Credentials
  {
    @Override
    public Request addCredentials(Request builder)
    {
      BasicEscalatorCredential basicEscalatorCredential = getOrUpdateEscalatorCredentials();
      LOG.debug("----------- Adding escalator credentials. username: %s", basicEscalatorCredential.getUsername());
      return builder.setBasicAuthentication(
          basicEscalatorCredential.getUsername(),
          basicEscalatorCredential.getPassword()
      );
    }
  }

  private BasicEscalatorCredential getOrUpdateEscalatorCredentials()
  {
    BasicEscalatorCredential escalatorCredential = cachedEscalatorCredential.get();
    BasicEscalatorCredential polledEscalatorCredential;
    long now = System.currentTimeMillis();
    long cutoff = now - (internalClientCredentialPoll * 1000L);

    if (lastVerified.get() < cutoff) {
      lastVerified.set(now);
      try {
        polledEscalatorCredential = cacheManager.get().getEscalatorCredential();
      }
      catch (Exception ex) {
        polledEscalatorCredential = null;
      }

      if (polledEscalatorCredential == null || polledEscalatorCredential.equals(escalatorCredential)) {
        LOG.debug("----------- Escalator credentials validated, no need to modify");
        return escalatorCredential;
      } else {
        LOG.debug("----------- Modified escalator credentials found, reloading");
        cachedEscalatorCredential.set(polledEscalatorCredential);
        return polledEscalatorCredential;
      }
    }
    return escalatorCredential;
  }
}
