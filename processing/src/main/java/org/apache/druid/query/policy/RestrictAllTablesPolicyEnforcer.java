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

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * All tables must be restricted by a policy.
 */
public class RestrictAllTablesPolicyEnforcer implements PolicyEnforcer
{
  private final ImmutableList<String> allowedPolicies;

  @JsonCreator
  public RestrictAllTablesPolicyEnforcer(
      @Nullable @JsonProperty("allowedPolicies") List<String> allowedPolicies
  )
  {
    if (allowedPolicies == null) {
      this.allowedPolicies = ImmutableList.of();
    } else {
      this.allowedPolicies = ImmutableList.copyOf(allowedPolicies);
    }
  }

  @Override
  public boolean validate(Policy policy)
  {
    return policy != null && (allowedPolicies.isEmpty() || allowedPolicies.contains(policy.getClass().getName()));
  }

  @JsonProperty
  public ImmutableList<String> getAllowedPolicies()
  {
    return allowedPolicies;
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
    RestrictAllTablesPolicyEnforcer that = (RestrictAllTablesPolicyEnforcer) o;
    return allowedPolicies.equals(that.allowedPolicies);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(allowedPolicies);
  }

  @Override
  public String toString()
  {
    return "RestrictAllTablesPolicyEnforcer{" +
           "allowedPolicies=" + allowedPolicies +
           '}';
  }
}
