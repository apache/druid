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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.segment.RestrictedSegment;
import org.apache.druid.segment.SegmentReference;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Reperesents a TableDataSource with policy restriction.
 * <p>
 * A RestrictedDataSource means the base TableDataSource has policy imposed. A table without any policy should never be
 * transformed to a RestrictedDataSource. Druid internal system and admin users would have a {@link NoRestrictionPolicy}.
 */
public class RestrictedDataSource implements DataSource
{
  private final TableDataSource base;

  private final Policy policy;

  @JsonProperty("base")
  public TableDataSource getBase()
  {
    return base;
  }

  @JsonProperty("policy")
  public Policy getPolicy()
  {
    return policy;
  }

  RestrictedDataSource(TableDataSource base, Policy policy)
  {
    this.base = base;
    this.policy = policy;
  }

  @JsonCreator
  public static RestrictedDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("policy") Policy policy
  )
  {
    if (!(base instanceof TableDataSource)) {
      throw new IAE("Expected a TableDataSource, got data source type [%s]", base.getClass());
    }
    if (Objects.isNull(policy)) {
      throw new IAE("Policy can't be null for RestrictedDataSource");
    }
    return new RestrictedDataSource((TableDataSource) base, policy);
  }

  @Override
  public Set<String> getTableNames()
  {
    return base.getTableNames();
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.of(base);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 1) {
      throw new IAE("Expected [1] child, got [%d]", children.size());
    }

    return create(children.get(0), policy);
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return base.isGlobal();
  }

  @Override
  public boolean isProcessable()
  {
    return base.isProcessable();
  }

  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(Query query)
  {
    final Function<SegmentReference, SegmentReference> segmentMapFn = base.createSegmentMapFunction(query);
    return baseSegment -> new RestrictedSegment(segmentMapFn.apply(baseSegment), policy);
  }

  @Override
  public DataSource withPolicies(Map<String, Optional<Policy>> policyMap)
  {
    if (!policyMap.containsKey(base.getName())) {
      throw new ISE("Missing policy check result for table [%s]", base.getName());
    }

    Optional<Policy> newPolicy = policyMap.getOrDefault(base.getName(), Optional.empty());
    if (!newPolicy.isPresent()) {
      throw new ISE(
          "No restriction found on table [%s], but had policy [%s] before.",
          base.getName(),
          policy
      );
    }
    if (newPolicy.get() instanceof NoRestrictionPolicy) {
      // druid-internal calls with NoRestrictionPolicy: allow
    } else if (newPolicy.get().equals(policy)) {
      // same policy: allow
    } else {
      throw new ISE(
          "Different restrictions on table [%s]: previous policy [%s] and new policy [%s]",
          base.getName(),
          policy,
          newPolicy.get()
      );
    }
    // The only happy path is, newPolicy is NoRestrictionPolicy, which means this comes from an anthenticated and
    // authorized druid-internal request.
    return this;
  }

  @Override
  public String toString()
  {
    return "RestrictedDataSource{" +
           "base=" + base +
           ", policy=" + policy + "}";
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
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
    RestrictedDataSource that = (RestrictedDataSource) o;
    return Objects.equals(base, that.base) && Objects.equals(policy, that.policy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, policy);
  }
}
