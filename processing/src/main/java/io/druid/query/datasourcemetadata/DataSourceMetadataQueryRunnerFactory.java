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

package io.druid.query.datasourcemetadata;

import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class DataSourceMetadataQueryRunnerFactory
    implements QueryRunnerFactory<Result<DataSourceMetadataResultValue>, DataSourceMetadataQuery>
{
  private static final DataSourceQueryQueryToolChest toolChest = new DataSourceQueryQueryToolChest();
  private final QueryWatcher queryWatcher;

  @Inject
  public DataSourceMetadataQueryRunnerFactory(QueryWatcher queryWatcher)
  {
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<DataSourceMetadataResultValue>> createRunner(final Segment segment)
  {
    return new DataSourceMetadataQueryRunner(segment);
  }

  @Override
  public QueryRunner<Result<DataSourceMetadataResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<DataSourceMetadataResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(
        queryExecutor, queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<DataSourceMetadataResultValue>, DataSourceMetadataQuery> getToolchest()
  {
    return toolChest;
  }

  private static class DataSourceMetadataQueryRunner implements QueryRunner<Result<DataSourceMetadataResultValue>>
  {
    private final StorageAdapter adapter;

    public DataSourceMetadataQueryRunner(Segment segment)
    {
      this.adapter = segment.asStorageAdapter();
    }

    @Override
    public Sequence<Result<DataSourceMetadataResultValue>> run(
        Query<Result<DataSourceMetadataResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof DataSourceMetadataQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass().getCanonicalName(), DataSourceMetadataQuery.class);
      }

      final DataSourceMetadataQuery legacyQuery = (DataSourceMetadataQuery) input;

      return new BaseSequence<>(
          new BaseSequence.IteratorMaker<Result<DataSourceMetadataResultValue>, Iterator<Result<DataSourceMetadataResultValue>>>()
          {
            @Override
            public Iterator<Result<DataSourceMetadataResultValue>> make()
            {
              if (adapter == null) {
                throw new ISE(
                    "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
                );
              }

              return legacyQuery.buildResult(
                  adapter.getInterval().getStart(),
                  adapter.getMaxIngestedEventTime()
              ).iterator();
            }

            @Override
            public void cleanup(Iterator<Result<DataSourceMetadataResultValue>> toClean)
            {

            }
          }
      );
    }
  }
}
