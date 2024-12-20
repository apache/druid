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
import org.apache.druid.query.filter.DimFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Represents a granular-level (e.x. row filter) restriction on read-table access.
 */
public class Policy
{
  /**
   * Defines how strict we want to enforce the policy on tables during query execution process.
   * <ol>
   *   <li>{@code APPLY_WHEN_APPLICABLE}, the most basic level, restriction is applied whenever seen fit.
   *   <li>{@code POLICY_CHECKED_ON_ALL_TABLES_ALLOW_EMPTY}, every table must have been checked on the policy.
   *   <li>{@code POLICY_CHECKED_ON_ALL_TABLES_POLICY_MUST_EXIST}, every table must have a policy when requests come from external users.
   * </ol>
   */
  public enum TablePolicySecurityLevel
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

  /**
   * A special kind of policy restriction, indicating that this table is restricted, but doesn't impose any restriction
   * to a user.
   */
  public static final Policy NO_RESTRICTION = new Policy(null);

  @JsonProperty("rowFilter")
  private final DimFilter rowFilter;

  @JsonCreator
  Policy(@Nullable @JsonProperty("rowFilter") DimFilter rowFilter)
  {
    this.rowFilter = rowFilter;
  }

  public static Policy fromRowFilter(@Nonnull DimFilter rowFilter)
  {
    return new Policy(rowFilter);
  }

  @Nullable
  public DimFilter getRowFilter()
  {
    return rowFilter;
  }

  /**
   * Returns true if the policy imposes no restrictions.
   */
  public boolean hasNoRestriction()
  {
    if (NO_RESTRICTION.equals(this)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString()
  {
    return "Policy{" + "rowFilter=" + rowFilter + '}';
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
    Policy that = (Policy) o;
    return Objects.equals(rowFilter, that.rowFilter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowFilter);
  }
}
