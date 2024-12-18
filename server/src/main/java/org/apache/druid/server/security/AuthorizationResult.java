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
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.server.Policy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

  private final Map<String, Policy> policyFilters;

  @Nullable
  private final Set<ResourceAction> sqlResourceActions;

  @Nullable
  private final Set<ResourceAction> allResourceActions;

  AuthorizationResult(
      boolean isAllowed,
      @Nullable String failureMessage,
      Map<String, Policy> policyFilters,
      @Nullable Set<ResourceAction> sqlResourceActions,
      @Nullable Set<ResourceAction> allResourceActions
  )
  {
    this.isAllowed = isAllowed;
    this.failureMessage = failureMessage;
    this.policyFilters = policyFilters;
    this.sqlResourceActions = sqlResourceActions;
    this.allResourceActions = allResourceActions;
  }

  public static AuthorizationResult deny(@Nonnull String failureMessage)
  {
    return new AuthorizationResult(false, failureMessage, Collections.emptyMap(), null, null);
  }

  public static AuthorizationResult allowWithRestriction(Map<String, Policy> policyFilters)
  {
    return new AuthorizationResult(true, null, policyFilters, null, null);
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

  public Optional<String> getPermissionErrorMessage(boolean policyFilterNotPermitted)
  {
    if (!isAllowed) {
      return Optional.of(Objects.requireNonNull(failureMessage));
    }

    if (policyFilterNotPermitted && policyFilters.values()
                                                 .stream().map(Policy::getRowFilter)
                                                 .flatMap(Optional::stream)
                                                 .anyMatch(filter -> !(filter instanceof TrueDimFilter))) {
      return Optional.of(Access.DEFAULT_ERROR_MESSAGE);
    }

    return Optional.empty();
  }

  public Map<String, Policy> getPolicy()
  {
    return policyFilters;
  }

  public Map<String, Optional<DimFilter>> getPolicyFilters()
  {
    Map<String, Optional<DimFilter>> filters = new HashMap<>();
    for (Map.Entry<String, Policy> entry : policyFilters.entrySet()) {
      filters.put(entry.getKey(), entry.getValue().getRowFilter());
    }
    return filters;
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
