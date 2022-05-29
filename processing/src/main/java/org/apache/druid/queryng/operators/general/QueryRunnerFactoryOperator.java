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

package org.apache.druid.queryng.operators.general;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.function.Supplier;

/**
 * Operator which wraps a query runner factory. When used in the interim
 * "shim" architecture, this operator allows a query runner to be
 * the input (upstream) to some other (downstream) operator. The
 * upstream query runner may give rise to its own operator. In that
 * case, at runtime, the sequence wrapper for that operator is
 * optimized away, leaving just the two operators.
 */
public class QueryRunnerFactoryOperator<T> implements Operator<T>
{
  private final FragmentContext context;
  private final Supplier<QueryRunner<T>> factory;
  private final QueryPlus<T> query;
  private Operator<T> child;

  public QueryRunnerFactoryOperator(Supplier<QueryRunner<T>> factory, QueryPlus<T> query)
  {
    this.context = query.fragment();
    this.factory = factory;
    this.query = query;
    context.register(this);
    context.updateProfile(this, OperatorProfile.silentOperator(this));
  }

  @Override
  public ResultIterator<T> open()
  {
    QueryRunner<T> runner = factory.get();
    Sequence<T> seq = runner.run(query, context.responseContext());
    child = Operators.toOperator(context, seq);
    context.registerChild(this, child);
    return child.open();
  }

  @Override
  public void close(boolean cascade)
  {
    if (child != null && cascade) {
      child.close(cascade);
    }
    child = null;
  }
}
