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

package org.apache.druid.query.policy;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.segment.CursorBuildSpec;

/**
 * Extensible interface for a granular-level (e.x. row filter) restriction on read-table access.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = RowFilterPolicy.class, name = "row"),
    @JsonSubTypes.Type(value = NoRestrictionPolicy.class, name = "noRestriction")
})
public interface Policy
{
  /**
   * Apply this policy to a {@link CursorBuildSpec} to seamlessly enforce policies for cursor-based queries. The
   * application must encapsulate 100% of the requirements of this policy.
   */
  CursorBuildSpec visit(CursorBuildSpec spec);

  /**
   * Defines how strict we want to enforce the policy on tables during query execution process.
   * <ol>
   *   <li>{@code APPLY_WHEN_APPLICABLE}, the most basic level, restriction is applied whenever seen fit.
   *   <li>{@code POLICY_CHECKED_ON_ALL_TABLES_ALLOW_EMPTY}, every table must have been checked on the policy.
   *   <li>{@code POLICY_CHECKED_ON_ALL_TABLES_POLICY_MUST_EXIST}, every table must have a policy when requests come from external users.
   * </ol>
   */
  enum TablePolicySecurityLevel
  {
    APPLY_WHEN_APPLICABLE(0),
    POLICY_CHECKED_ON_ALL_TABLES_ALLOW_EMPTY(1),
    POLICY_CHECKED_ON_ALL_TABLES_POLICY_MUST_EXIST(2);

    private final int securityLevel;

    TablePolicySecurityLevel(int securityLevel)
    {
      this.securityLevel = securityLevel;
    }

    /**
     * Returns true if the security level requires that, every table must have an entry in the policy map during query
     * execution stage.
     */
    public boolean policyMustBeCheckedOnAllTables()
    {
      return securityLevel >= 1;
    }

    /**
     * Returns true if the security level requires that, every table must have a policy during query execution stage,
     * this means the table must have a non-empty value in the policy map.
     */
    public boolean policyMustBeCheckedAndExistOnAllTables()
    {
      return securityLevel >= 2;
    }
  }
}
