/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.aggregation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class Aggregation
{
  private final List<VirtualColumn> virtualColumns;
  private final List<AggregatorFactory> aggregatorFactories;
  private final PostAggregator postAggregator;

  private Aggregation(
      final List<VirtualColumn> virtualColumns,
      final List<AggregatorFactory> aggregatorFactories,
      final PostAggregator postAggregator
  )
  {
    this.virtualColumns = Preconditions.checkNotNull(virtualColumns, "virtualColumns");
    this.aggregatorFactories = Preconditions.checkNotNull(aggregatorFactories, "aggregatorFactories");
    this.postAggregator = postAggregator;

    if (postAggregator == null) {
      Preconditions.checkArgument(aggregatorFactories.size() == 1, "aggregatorFactories.size == 1");
    } else {
      // Verify that there are no "useless" fields in the aggregatorFactories.
      // Don't verify that the PostAggregator inputs are all present; they might not be.
      final Set<String> dependentFields = postAggregator.getDependentFields();
      for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
        if (!dependentFields.contains(aggregatorFactory.getName())) {
          throw new IAE("Unused field[%s] in Aggregation", aggregatorFactory.getName());
        }
      }
    }

    // Verify that all "internal" aggregator names are prefixed by the output name of this aggregation.
    // This is a sanity check to make sure callers are behaving as they should be.
    final String name = postAggregator != null
                        ? postAggregator.getName()
                        : Iterables.getOnlyElement(aggregatorFactories).getName();

    for (VirtualColumn virtualColumn : virtualColumns) {
      if (!virtualColumn.getOutputName().startsWith(name)) {
        throw new IAE("VirtualColumn[%s] not prefixed under[%s]", virtualColumn.getOutputName(), name);
      }
    }

    for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
      if (!aggregatorFactory.getName().startsWith(name)) {
        throw new IAE("Aggregator[%s] not prefixed under[%s]", aggregatorFactory.getName(), name);
      }
    }
  }

  public static Aggregation create(final List<VirtualColumn> virtualColumns, final AggregatorFactory aggregatorFactory)
  {
    return new Aggregation(
        virtualColumns,
        ImmutableList.of(aggregatorFactory),
        null
    );
  }

  public static Aggregation create(final AggregatorFactory aggregatorFactory)
  {
    return new Aggregation(
        ImmutableList.of(),
        ImmutableList.of(aggregatorFactory),
        null
    );
  }

  public static Aggregation create(final PostAggregator postAggregator)
  {
    return new Aggregation(ImmutableList.of(), ImmutableList.of(), postAggregator);
  }

  public static Aggregation create(
      final List<AggregatorFactory> aggregatorFactories,
      final PostAggregator postAggregator
  )
  {
    return new Aggregation(ImmutableList.of(), aggregatorFactories, postAggregator);
  }

  public static Aggregation create(
      final List<VirtualColumn> virtualColumns,
      final List<AggregatorFactory> aggregatorFactories,
      final PostAggregator postAggregator
  )
  {
    return new Aggregation(virtualColumns, aggregatorFactories, postAggregator);
  }

  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  public List<AggregatorFactory> getAggregatorFactories()
  {
    return aggregatorFactories;
  }

  public PostAggregator getPostAggregator()
  {
    return postAggregator;
  }

  public String getOutputName()
  {
    return postAggregator != null
           ? postAggregator.getName()
           : Iterables.getOnlyElement(aggregatorFactories).getName();
  }

  public Aggregation filter(final DimFilter filter)
  {
    if (filter == null) {
      return this;
    }

    if (postAggregator != null) {
      // Verify that this Aggregation contains all input to its postAggregator.
      // If not, this "filter" call won't work right.
      final Set<String> dependentFields = postAggregator.getDependentFields();
      final Set<String> aggregatorNames = Sets.newHashSet();
      for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
        aggregatorNames.add(aggregatorFactory.getName());
      }
      for (String field : dependentFields) {
        if (!aggregatorNames.contains(field)) {
          throw new ISE("Cannot filter an Aggregation that does not contain its inputs: %s", this);
        }
      }
    }

    final List<AggregatorFactory> newAggregators = Lists.newArrayList();
    for (AggregatorFactory agg : aggregatorFactories) {
      newAggregators.add(new FilteredAggregatorFactory(agg, filter));
    }

    return new Aggregation(virtualColumns, newAggregators, postAggregator);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Aggregation that = (Aggregation) o;
    return Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(aggregatorFactories, that.aggregatorFactories) &&
           Objects.equals(postAggregator, that.postAggregator);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(virtualColumns, aggregatorFactories, postAggregator);
  }

  @Override
  public String toString()
  {
    return "Aggregation{" +
           "virtualColumns=" + virtualColumns +
           ", aggregatorFactories=" + aggregatorFactories +
           ", postAggregator=" + postAggregator +
           '}';
  }
}
