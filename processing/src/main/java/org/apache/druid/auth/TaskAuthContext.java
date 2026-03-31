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

package org.apache.druid.auth;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.ExtensionPoint;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Holds authentication context that can be passed to tasks for accessing external services
 * that require user credentials (e.g., Iceberg REST Catalog with OAuth).
 *
 * <p>Implementations must ensure credentials are redacted during serialization by applying
 * {@link TaskAuthContextRedactionMixIn} to the ObjectMapper used for persistence/logging.
 * This follows the same pattern as {@link org.apache.druid.metadata.PasswordProvider} and
 * {@link org.apache.druid.metadata.PasswordProviderRedactionMixIn}.
 *
 * <p>The auth context is injected into tasks at submission time by {@code TaskAuthContextProvider}
 * and is available during task execution. It is NOT persisted to metadata storage - only the
 * non-sensitive fields (identity, metadata) are serialized.
 *
 * @see TaskAuthContextRedactionMixIn
 */
@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface TaskAuthContext
{
  /**
   * Returns the authenticated identity (e.g., username, email, service account name).
   * This value is safe to serialize and log.
   *
   * @return the identity string
   */
  String getIdentity();

  /**
   * Returns sensitive credentials (e.g., OAuth tokens, API keys).
   * This method MUST be redacted during serialization via {@link TaskAuthContextRedactionMixIn}.
   *
   * <p>The map keys are credential identifiers (e.g., "token", "oauth2-token") and values
   * are the actual credential strings. The specific keys expected depend on the consumer
   * (e.g., Iceberg REST Catalog may expect "token").
   *
   * @return map of credential name to credential value, or null if no credentials
   */
  @Nullable
  Map<String, String> getCredentials();

  /**
   * Returns non-sensitive metadata about the auth context (e.g., token expiry time, scopes, roles).
   * This value is safe to serialize and log.
   *
   * @return map of metadata, or null if no metadata
   */
  @Nullable
  default Map<String, Object> getMetadata()
  {
    return null;
  }
}
