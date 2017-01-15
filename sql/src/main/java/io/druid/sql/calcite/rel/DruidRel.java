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

import com.google.common.base.Function;
import io.druid.query.QueryDataSource;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

public abstract class DruidRel<T extends DruidRel> extends AbstractRelNode implements BindableRel
{
  private final QueryMaker queryMaker;

  public DruidRel(RelOptCluster cluster, RelTraitSet traitSet, QueryMaker queryMaker)
  {
    super(cluster, traitSet);
    this.queryMaker = queryMaker;
  }

  public abstract RowSignature getSourceRowSignature();

  public final RowSignature getOutputRowSignature()
  {
    return getQueryBuilder().getOutputRowSignature();
  }

  public abstract DruidQueryBuilder getQueryBuilder();

  /**
   * Return the number of Druid queries this rel involves, including sub-queries. Simple queries will return 1.
   *
   * @return number of nested queries
   */
  public abstract int getQueryCount();

  public abstract void accumulate(Function<Row, Void> sink);

  public abstract T withQueryBuilder(DruidQueryBuilder newQueryBuilder);

  /**
   * Convert this DruidRel to a QueryDataSource, for embedding in some other outer query. This may be an expensive
   * operation. For example, DruidSemiJoin needs to execute the right-hand side query in order to complete this
   * method.
   *
   * This method may return null if it knows that this rel will yield an empty result set.
   *
   * @return query dataSource, or null if it is known in advance that this rel will yield an empty result set.
   */
  public abstract QueryDataSource asDataSource();

  public abstract T asBindable();

  public QueryMaker getQueryMaker()
  {
    return queryMaker;
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
        accumulate(QueryMaker.sinkFunction(sink));
      }
    };
  }

  @Override
  public Enumerable<Object[]> bind(final DataContext dataContext)
  {
    throw new UnsupportedOperationException();
  }
}
