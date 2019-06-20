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

package org.apache.druid.sql.calcite.aggregation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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

    if (aggregatorFactories.isEmpty()) {
      Preconditions.checkArgument(postAggregator != null, "postAggregator must be present if there are no aggregators");
    }

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
    return new Aggregation(Collections.emptyList(), Collections.emptyList(), postAggregator);
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

  @Nullable
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

  public Aggregation filter(
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final DimFilter filter
  )
  {
    if (filter == null) {
      return this;
    }


    if (postAggregator != null) {
      // Verify that this Aggregation contains all input to its postAggregator.
      // If not, this "filter" call won't work right.
      final Set<String> dependentFields = postAggregator.getDependentFields();
      final Set<String> aggregatorNames = new HashSet<>();
      for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
        aggregatorNames.add(aggregatorFactory.getName());
      }
      for (String field : dependentFields) {
        if (!aggregatorNames.contains(field)) {
          throw new ISE("Cannot filter an Aggregation that does not contain its inputs: %s", this);
        }
      }
    }

    final DimFilter baseOptimizedFilter = Filtration.create(filter)
                                                    .optimizeFilterOnly(virtualColumnRegistry.getFullRowSignature())
                                                    .getDimFilter();

    Set<VirtualColumn> aggVirtualColumnsPlusFilterColumns = new HashSet<>(virtualColumns);
    for (String column : baseOptimizedFilter.getRequiredColumns()) {
      if (virtualColumnRegistry.isVirtualColumnDefined(column)) {
        aggVirtualColumnsPlusFilterColumns.add(virtualColumnRegistry.getVirtualColumn(column));
      }
    }
    final List<AggregatorFactory> newAggregators = new ArrayList<>();
    for (AggregatorFactory agg : aggregatorFactories) {
      if (agg instanceof FilteredAggregatorFactory) {
        final FilteredAggregatorFactory filteredAgg = (FilteredAggregatorFactory) agg;
        for (String column : filteredAgg.getFilter().getRequiredColumns()) {
          if (virtualColumnRegistry.isVirtualColumnDefined(column)) {
            aggVirtualColumnsPlusFilterColumns.add(virtualColumnRegistry.getVirtualColumn(column));
          }
        }
        newAggregators.add(
            new FilteredAggregatorFactory(
                filteredAgg.getAggregator(),
                Filtration.create(new AndDimFilter(ImmutableList.of(filteredAgg.getFilter(), baseOptimizedFilter)))
                          .optimizeFilterOnly(virtualColumnRegistry.getFullRowSignature())
                          .getDimFilter()
            )
        );
      } else {
        newAggregators.add(new FilteredAggregatorFactory(agg, baseOptimizedFilter));
      }
    }

    return new Aggregation(new ArrayList<>(aggVirtualColumnsPlusFilterColumns), newAggregators, postAggregator);
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
