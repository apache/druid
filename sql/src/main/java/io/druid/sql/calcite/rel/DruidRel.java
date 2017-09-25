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

package io.druid.sql.calcite.rel;

import com.google.common.base.Throwables;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.sql.calcite.planner.PlannerContext;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

import javax.annotation.Nullable;
import java.util.List;

public abstract class DruidRel<T extends DruidRel> extends AbstractRelNode implements BindableRel
{
  private final QueryMaker queryMaker;

  protected DruidRel(RelOptCluster cluster, RelTraitSet traitSet, QueryMaker queryMaker)
  {
    super(cluster, traitSet);
    this.queryMaker = queryMaker;
  }

  public abstract PartialDruidQuery getPartialDruidQuery();

  /**
   * Return the number of Druid queries this rel involves, including sub-queries. Simple queries will return 1.
   *
   * @return number of nested queries
   */
  public abstract int getQueryCount();

  public abstract Sequence<Object[]> runQuery();

  public abstract T withPartialQuery(PartialDruidQuery newQueryBuilder);

  public boolean isValidDruidQuery()
  {
    try {
      toDruidQueryForExplaining();
      return true;
    }
    catch (CannotBuildQueryException e) {
      return false;
    }
  }

  /**
   * Convert this DruidRel to a DruidQuery. This may be an expensive operation. For example, DruidSemiJoin needs to
   * execute the right-hand side query in order to complete this method.
   *
   * This method may return null if it knows that this rel will yield an empty result set.
   *
   * @return query, or null if it is known in advance that this rel will yield an empty result set.
   *
   * @throws CannotBuildQueryException
   */
  @Nullable
  public abstract DruidQuery toDruidQuery();

  /**
   * Convert this DruidRel to a DruidQuery for purposes of explaining. This must be an inexpensive operation. For
   * example, DruidSemiJoin will use a dummy dataSource in order to complete this method, rather than executing
   * the right-hand side query.
   *
   * This method may not return null.
   *
   * @return query
   *
   * @throws CannotBuildQueryException
   */
  public abstract DruidQuery toDruidQueryForExplaining();

  public abstract T asBindable();

  public QueryMaker getQueryMaker()
  {
    return queryMaker;
  }

  public PlannerContext getPlannerContext()
  {
    return queryMaker.getPlannerContext();
  }

  public abstract T asDruidConvention();

  /**
   * Get a list of names of datasources read by this DruidRel
   */
  public abstract List<String> getDatasourceNames();

  @Override
  public Class<Object[]> getElementType()
  {
    return Object[].class;
  }

  @Override
  public Node implement(InterpreterImplementor implementor)
  {
    final Sink sink = implementor.interpreter.sink(this);
    return new Node()
    {
      @Override
      public void run() throws InterruptedException
      {
        runQuery().accumulate(
            sink,
            new Accumulator<Sink, Object[]>()
            {
              @Override
              public Sink accumulate(final Sink theSink, final Object[] in)
              {
                try {
                  theSink.send(Row.of(in));
                }
                catch (InterruptedException e) {
                  throw Throwables.propagate(e);
                }
                return theSink;
              }
            }
        );
      }
    };
  }

  @Override
  public Enumerable<Object[]> bind(final DataContext dataContext)
  {
    throw new UnsupportedOperationException();
  }
}
