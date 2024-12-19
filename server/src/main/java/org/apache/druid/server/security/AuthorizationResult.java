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

package org.apache.druid.server.security;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.policy.Policy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents the outcoming of performing authorization check on required resource accesses on a query or http requests.
 * It contains:
 * <ul>
 *   <li>a boolean allow or deny access results for checking permissions on a list of resource actions.
 *   <li>a failure message if deny access. It's null when access is allowed.
 *   <li>a map of table name with optional {@link Policy} restriction. An empty value means there's no restriction
 *   enforced on the table.
 * </ul>
 */
public class AuthorizationResult
{
  /**
   * Provides unrestricted access to all resources. This should be limited to Druid internal systems or superusers,
   * except in cases where ACL considerations are not a priority.
   */
  public static final AuthorizationResult ALLOW_ALL = new AuthorizationResult(
      true,
      null,
      Collections.emptyMap(),
      null,
      null
  );

  /**
   * Provides a default deny access result.
   */
  public static final AuthorizationResult DENY = new AuthorizationResult(
      false,
      Access.DENIED.getMessage(),
      Collections.emptyMap(),
      null,
      null
  );

  private final boolean isAllowed;

  @Nullable
  private final String failureMessage;

  private final Map<String, Optional<Policy>> policyRestrictions;

  @Nullable
  private final Set<ResourceAction> sqlResourceActions;

  @Nullable
  private final Set<ResourceAction> allResourceActions;

  AuthorizationResult(
      boolean isAllowed,
      @Nullable String failureMessage,
      Map<String, Optional<Policy>> policyRestrictions,
      @Nullable Set<ResourceAction> sqlResourceActions,
      @Nullable Set<ResourceAction> allResourceActions
  )
  {
    this.isAllowed = isAllowed;
    this.failureMessage = failureMessage;
    this.policyRestrictions = policyRestrictions;
    this.sqlResourceActions = sqlResourceActions;
    this.allResourceActions = allResourceActions;
  }

  public static AuthorizationResult deny(@Nonnull String failureMessage)
  {
    return new AuthorizationResult(false, failureMessage, Collections.emptyMap(), null, null);
  }

  public static AuthorizationResult allowWithRestriction(Map<String, Optional<Policy>> policyRestrictions)
  {
    return new AuthorizationResult(true, null, policyRestrictions, null, null);
  }

  public AuthorizationResult withResourceActions(
      Set<ResourceAction> sqlResourceActions,
      Set<ResourceAction> allResourceActions
  )
  {
    return new AuthorizationResult(
        isAllowed,
        failureMessage,
        ImmutableMap.copyOf(getPolicy()),
        sqlResourceActions,
        allResourceActions
    );
  }

  /**
   * Returns a permission error string if the AuthorizationResult doesn't permit all requried access. Otherwise, returns
   * empty. When {@code policyRestrictionsNotPermitted} set to true, it requests unrestricted full access. The caller
   * can use this method to retrieve the error string, and throw a {@link ForbiddenException} with the error message.
   * <p>
   * It first checks if all permissions (e.x. {@link org.apache.druid.security.basic.authorization.entity.BasicAuthorizerPermission})
   * have been granted access. If not, returns the {@code failureMessage}. Then if {@code policyRestrictionsNotPermitted},
   * it checks for 'actual' policy restrictions (i.e. {@link Policy#hasNoRestriction} returns false). If 'actual' policy
   * restrictions exist, returns {@link Access#DEFAULT_ERROR_MESSAGE}.
   *
   * @param policyRestrictionsNotPermitted true if policy restrictions are considered as not permitted
   * @return optional permission error message
   */
  public Optional<String> getPermissionErrorMessage(boolean policyRestrictionsNotPermitted)
  {
    if (!isAllowed) {
      return Optional.of(Objects.requireNonNull(failureMessage));
    }

    if (policyRestrictionsNotPermitted && policyRestrictions.values()
                                                            .stream()
                                                            .flatMap(policy -> policy.isPresent()
                                                                               ? Stream.of(policy.get())
                                                                               : Stream.empty()) // Can be replaced by Optional.stream after Java 11
                                                            .anyMatch(Policy::hasNoRestriction)) {
      return Optional.of(Access.DEFAULT_ERROR_MESSAGE);
    }

    return Optional.empty();
  }

  public Map<String, Optional<Policy>> getPolicy()
  {
    return policyRestrictions;
  }

  @Nullable
  public Set<ResourceAction> getSqlResourceActions()
  {
    return sqlResourceActions;
  }

  @Nullable
  public Set<ResourceAction> getAllResourceActions()
  {
    return allResourceActions;
  }
}
