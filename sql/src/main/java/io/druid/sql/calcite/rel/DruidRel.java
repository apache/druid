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
import io.druid.sql.calcite.table.DruidTable;
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
  public DruidRel(RelOptCluster cluster, RelTraitSet traitSet)
  {
    super(cluster, traitSet);
  }

  public abstract DruidTable getDruidTable();

  public abstract DruidQueryBuilder getQueryBuilder();

  public abstract void accumulate(Function<Row, Void> sink);

  public abstract T withQueryBuilder(DruidQueryBuilder newQueryBuilder);

  public abstract T asBindable();

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
