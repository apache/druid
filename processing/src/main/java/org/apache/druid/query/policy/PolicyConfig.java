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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Defines how strict we want to enforce the policy on tables during query execution process.
 */
public class PolicyConfig
{
  @JsonProperty
  private final TablePolicySecurityLevel tablePolicySecurityLevel;

  @JsonProperty
  private final ImmutableList<String> allowedPolicies;

  @JsonCreator
  public PolicyConfig(
      @JsonProperty("tablePolicySecurityLevel") TablePolicySecurityLevel tablePolicySecurityLevel,
      @JsonProperty("allowedPolicies") ImmutableList<String> allowedPolicies
  )
  {
    this.tablePolicySecurityLevel = tablePolicySecurityLevel;
    this.allowedPolicies = allowedPolicies;
  }

  /**
   * Returns a {@link PolicyConfig} instance that applies restrictions whenever seen fit, and allows all policy types.
   */
  public static PolicyConfig defaultInstance()
  {
    return new PolicyConfig(TablePolicySecurityLevel.APPLY_WHEN_APPLICABLE, ImmutableList.of());
  }

  /**
   * Returns the table policy security level, higher value means stricter policy.
   *
   * @see TablePolicySecurityLevel
   */
  @JsonProperty
  public TablePolicySecurityLevel getTablePolicySecurityLevel()
  {
    return tablePolicySecurityLevel;
  }

  /**
   * Returns a list of allowed policy class.
   * <p>
   * An empty list means all policy classes are allowed.
   */
  @JsonProperty
  public ImmutableList<String> getAllowedPolicies()
  {
    return allowedPolicies;
  }

  public boolean allowPolicy(@Nonnull Policy policy)
  {
    return allowedPolicies.isEmpty() || allowedPolicies.contains(policy.getClass().getSimpleName());
  }

  /**
   * Returns true if the security level requires that, every table must have a policy during query execution stage,
   * this means the table must have a non-empty value in the policy map.
   */
  public boolean policyMustBeCheckedAndExistOnAllTables()
  {
    return tablePolicySecurityLevel.securityLevel >= 2.0;
  }

  @Override
  public String toString()
  {
    return "PolicyConfig{tablePolicySecurityLevel="
           + tablePolicySecurityLevel
           + ", allowedPolicies="
           + allowedPolicies
           + '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PolicyConfig that = (PolicyConfig) o;
    return Objects.equals(tablePolicySecurityLevel, that.tablePolicySecurityLevel) &&
           Objects.equals(allowedPolicies, that.allowedPolicies);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tablePolicySecurityLevel, allowedPolicies);
  }


  /**
   * Defines how strict we want to enforce the policy on tables during query execution process.
   * <ol>
   *   <li>{@code APPLY_WHEN_APPLICABLE}, the most basic level, restriction is applied whenever seen fit.
   *   <li>{@code POLICY_CHECKED_ON_ALL_TABLES_POLICY_MUST_EXIST}, every table must have a policy when requests come from external users.
   * </ol>
   */
  public enum TablePolicySecurityLevel
  {
    @JsonProperty("0") APPLY_WHEN_APPLICABLE(0f),
    @JsonProperty("2.0f") POLICY_CHECKED_ON_ALL_TABLES_POLICY_MUST_EXIST(2.0f);

    private final float securityLevel;

    TablePolicySecurityLevel(float securityLevel)
    {
      this.securityLevel = securityLevel;
    }
  }
}
