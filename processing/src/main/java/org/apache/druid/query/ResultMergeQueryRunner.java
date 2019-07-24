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

import org.apache.druid.common.guava.CombiningSequence;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;

import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 */
@PublicApi
public class ResultMergeQueryRunner<T> extends BySegmentSkippingQueryRunner<T>
{
  private final Function<Query<T>, Comparator<T>> comparatorGenerator;
  private final Function<Query<T>, BinaryOperator<T>> mergeFnGenerator;

  public ResultMergeQueryRunner(
      QueryRunner<T> baseRunner,
      Function<Query<T>, Comparator<T>> comparatorGenerator,
      Function<Query<T>, BinaryOperator<T>> mergeFnGenerator
  )
  {
    super(baseRunner);
    this.comparatorGenerator = comparatorGenerator;
    this.mergeFnGenerator = mergeFnGenerator;
  }

  @Override
  public Sequence<T> doRun(QueryRunner<T> baseRunner, QueryPlus<T> queryPlus, ResponseContext context)
  {
    Query<T> query = queryPlus.getQuery();
    return CombiningSequence.create(
        baseRunner.run(queryPlus, context),
        comparatorGenerator.apply(query),
        mergeFnGenerator.apply(query));
  }
}
