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

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;

import java.util.Map;

public class UnionQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public UnionQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final Map<String, Object> responseContext)
  {
    Query<T> query = queryPlus.getQuery();
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof UnionDataSource) {

      return new MergeSequence<>(
          query.getResultOrdering(),
          Sequences.simple(
              Lists.transform(
                  ((UnionDataSource) dataSource).getDataSources(),
                  new Function<DataSource, Sequence<T>>()
                  {
                    @Override
                    public Sequence<T> apply(DataSource singleSource)
                    {
                      return baseRunner.run(
                          queryPlus.withQuery(query.withDataSource(singleSource)),
                          responseContext
                      );
                    }
                  }
              )
          )
      );
    } else {
      return baseRunner.run(queryPlus, responseContext);
    }
  }

}
