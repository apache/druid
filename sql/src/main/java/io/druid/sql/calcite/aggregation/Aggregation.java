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

import java.util.List;
import java.util.Set;

public class Aggregation
{
  private final List<AggregatorFactory> aggregatorFactories;
  private final PostAggregator postAggregator;
  private final PostAggregatorFactory finalizingPostAggregatorFactory;

  private Aggregation(
      final List<AggregatorFactory> aggregatorFactories,
      final PostAggregator postAggregator,
      final PostAggregatorFactory finalizingPostAggregatorFactory
  )
  {
    this.aggregatorFactories = Preconditions.checkNotNull(aggregatorFactories, "aggregatorFactories");
    this.postAggregator = postAggregator;
    this.finalizingPostAggregatorFactory = finalizingPostAggregatorFactory;

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
  }

  public static Aggregation create(final AggregatorFactory aggregatorFactory)
  {
    return new Aggregation(ImmutableList.of(aggregatorFactory), null, null);
  }

  public static Aggregation create(final PostAggregator postAggregator)
  {
    return new Aggregation(ImmutableList.<AggregatorFactory>of(), postAggregator, null);
  }

  public static Aggregation create(
      final List<AggregatorFactory> aggregatorFactories,
      final PostAggregator postAggregator
  )
  {
    return new Aggregation(aggregatorFactories, postAggregator, null);
  }

  public static Aggregation createFinalizable(
      final List<AggregatorFactory> aggregatorFactories,
      final PostAggregator postAggregator,
      final PostAggregatorFactory finalizingPostAggregatorFactory
  )
  {
    return new Aggregation(
        aggregatorFactories,
        postAggregator,
        Preconditions.checkNotNull(finalizingPostAggregatorFactory, "finalizingPostAggregatorFactory")
    );
  }

  public List<AggregatorFactory> getAggregatorFactories()
  {
    return aggregatorFactories;
  }

  public PostAggregator getPostAggregator()
  {
    return postAggregator;
  }

  public PostAggregatorFactory getFinalizingPostAggregatorFactory()
  {
    return finalizingPostAggregatorFactory;
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
      // Verify that this Aggregation contains all inputs. If not, this "filter" call won't work right.
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

    return new Aggregation(
        newAggregators,
        postAggregator,
        finalizingPostAggregatorFactory
    );
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

    Aggregation that = (Aggregation) o;

    if (aggregatorFactories != null
        ? !aggregatorFactories.equals(that.aggregatorFactories)
        : that.aggregatorFactories != null) {
      return false;
    }
    if (postAggregator != null ? !postAggregator.equals(that.postAggregator) : that.postAggregator != null) {
      return false;
    }
    return finalizingPostAggregatorFactory != null
           ? finalizingPostAggregatorFactory.equals(that.finalizingPostAggregatorFactory)
           : that.finalizingPostAggregatorFactory == null;
  }

  @Override
  public int hashCode()
  {
    int result = aggregatorFactories != null ? aggregatorFactories.hashCode() : 0;
    result = 31 * result + (postAggregator != null ? postAggregator.hashCode() : 0);
    result = 31 * result + (finalizingPostAggregatorFactory != null ? finalizingPostAggregatorFactory.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Aggregation{" +
           "aggregatorFactories=" + aggregatorFactories +
           ", postAggregator=" + postAggregator +
           ", finalizingPostAggregatorFactory=" + finalizingPostAggregatorFactory +
           '}';
  }
}
