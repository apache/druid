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

package org.apache.druid.security.basic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Basic authentication storage/cache/resource handler config.
 * BasicAuthClassCompositionConfig provides options to specify authenticator/authorizer classes of user/role managers,
 * caches and notifiers. If a field in this class is non-null then the corresponding class is instantiated
 * regardless of what type of Druid component runs it (see {@link BasicSecurityDruidModule}).
 * Hence every Druid component might be a user/role manager and notify others by sending notifications.
 * Every field must be a valid class name (appropriate for the corresponding goal) or null.
 */
public class BasicAuthClassCompositionConfig
{
  @JsonProperty
  private final String authenticatorMetadataStorageUpdater;

  @JsonProperty
  private final String authenticatorCacheManager;

  @JsonProperty
  private final String authenticatorResourceHandler;

  @JsonProperty
  private final String authenticatorCacheNotifier;

  @JsonProperty
  private final String authorizerMetadataStorageUpdater;

  @JsonProperty
  private final String authorizerCacheManager;

  @JsonProperty
  private final String authorizerResourceHandler;

  @JsonProperty
  private final String authorizerCacheNotifier;

  @JsonCreator
  public BasicAuthClassCompositionConfig(
      @JsonProperty("authenticatorMetadataStorageUpdater") String authenticatorMetadataStorageUpdater,
      @JsonProperty("authenticatorCacheManager") String authenticatorCacheManager,
      @JsonProperty("authenticatorResourceHandler") String authenticatorResourceHandler,
      @JsonProperty("authenticatorCacheNotifier") String authenticatorCacheNotifier,
      @JsonProperty("authorizerMetadataStorageUpdater") String authorizerMetadataStorageUpdater,
      @JsonProperty("authorizerCacheManager") String authorizerCacheManager,
      @JsonProperty("authorizerResourceHandler") String authorizerResourceHandler,
      @JsonProperty("authorizerCacheNotifier") String authorizerCacheNotifier
  )
  {
    this.authenticatorMetadataStorageUpdater = authenticatorMetadataStorageUpdater;
    this.authenticatorCacheManager = authenticatorCacheManager;
    this.authenticatorResourceHandler = authenticatorResourceHandler;
    this.authenticatorCacheNotifier = authenticatorCacheNotifier;
    this.authorizerMetadataStorageUpdater = authorizerMetadataStorageUpdater;
    this.authorizerCacheManager = authorizerCacheManager;
    this.authorizerResourceHandler = authorizerResourceHandler;
    this.authorizerCacheNotifier = authorizerCacheNotifier;
  }

  @JsonProperty
  public String getAuthenticatorMetadataStorageUpdater()
  {
    return authenticatorMetadataStorageUpdater;
  }

  @JsonProperty
  public String getAuthenticatorCacheManager()
  {
    return authenticatorCacheManager;
  }

  @JsonProperty
  public String getAuthenticatorResourceHandler()
  {
    return authenticatorResourceHandler;
  }

  @JsonProperty
  public String getAuthenticatorCacheNotifier()
  {
    return authenticatorCacheNotifier;
  }

  @JsonProperty
  public String getAuthorizerMetadataStorageUpdater()
  {
    return authorizerMetadataStorageUpdater;
  }

  @JsonProperty
  public String getAuthorizerCacheManager()
  {
    return authorizerCacheManager;
  }

  @JsonProperty
  public String getAuthorizerResourceHandler()
  {
    return authorizerResourceHandler;
  }

  @JsonProperty
  public String getAuthorizerCacheNotifier()
  {
    return authorizerCacheNotifier;
  }
}
