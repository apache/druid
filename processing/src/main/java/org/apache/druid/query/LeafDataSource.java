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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.FilterSegmentPruner;
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Leaf {@link DataSource}-s have no inputs.
 */
public abstract class LeafDataSource implements DataSource
{
  @Override
  public final List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public final DataSource withChildren(List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public SegmentMapFunction createSegmentMapFunction(Query query)
  {
    return SegmentMapFunction.IDENTITY;
  }

  @Nullable
  @Override
  public SegmentPruner createSegmentPruner(@Nullable DimFilter filter, VirtualColumns virtualColumns)
  {
    return filter == null ? null : new FilterSegmentPruner(filter, null, virtualColumns);
  }

  @Override
  public DataSource withPolicies(Map<String, Optional<Policy>> policyMap, PolicyEnforcer policyEnforcer)
  {
    if (getTableNames().isEmpty()) {
      // No need to call the default method, because a leaf with no tables has nothing to apply policies to.
      return this;
    } else {
      throw DruidException.defensive("Class[%s] can have tables, it must override withPolicies", getClass());
    }
  }

  @Override
  public Set<String> getReservedColumns()
  {
    return Set.of();
  }

  @Override
  public boolean mayFilterRows()
  {
    return false;
  }
}
