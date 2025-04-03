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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.segment.RestrictedSegment;
import org.apache.druid.segment.Segment;
import org.reflections.Reflections;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
      Set<String> policyClazz = new Reflections("org.apache.druid.query.policy").getSubTypesOf(
          Policy.class).stream().map(Class::getSimpleName).collect(Collectors.toSet());

      List<String> unrecognizedClazz = allowedPolicies.stream()
                                                      .filter(p -> !policyClazz.contains(p))
                                                      .collect(Collectors.toList());
      Preconditions.checkArgument(
          unrecognizedClazz.isEmpty(),
          "Unrecognized policy class[%s], must be one of class[%s]",
          unrecognizedClazz,
          policyClazz
      );
      this.allowedPolicies = ImmutableList.copyOf(allowedPolicies);
    }
  }

  @Override
  public boolean validate(DataSource ds, Policy policy)
  {
    if (ds instanceof RestrictedDataSource) {
      return allowedPolicies.isEmpty() || allowedPolicies.contains(policy.getClass().getSimpleName());
    }
    return false;
  }

  @Override
  public boolean validate(Segment segment, Policy policy)
  {
    if (segment instanceof RestrictedSegment) {
      return allowedPolicies.isEmpty() || allowedPolicies.contains(policy.getClass().getSimpleName());
    }
    return false;
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
