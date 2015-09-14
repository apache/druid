/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;

import java.util.Map;

public class UnionQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;

  public UnionQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof UnionDataSource) {
      return toolChest.mergeSequencesUnordered(
          Sequences.simple(
              Lists.transform(
                  ((UnionDataSource) dataSource).getDataSources(),
                  new Function<DataSource, Sequence<T>>()
                  {
                    @Override
                    public Sequence<T> apply(DataSource singleSource)
                    {
                      return baseRunner.run(
                          query.withDataSource(singleSource),
                          responseContext
                      );
                    }
                  }
              )
          )
      );
    } else {
      return baseRunner.run(query, responseContext);
    }
  }

}
