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

package org.apache.druid.query.datasourcemetadata;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.Result;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;

/**
 */
public class DataSourceMetadataQueryRunnerFactory
    implements QueryRunnerFactory<Result<DataSourceMetadataResultValue>, DataSourceMetadataQuery>
{
  private final DataSourceQueryQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;

  @Inject
  public DataSourceMetadataQueryRunnerFactory(
      DataSourceQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<DataSourceMetadataResultValue>> createRunner(final Segment segment)
  {
    return new DataSourceMetadataQueryRunner(segment);
  }

  @Override
  public QueryRunner<Result<DataSourceMetadataResultValue>> mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<Result<DataSourceMetadataResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(queryExecutor, queryWatcher, queryRunners);
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
        QueryPlus<Result<DataSourceMetadataResultValue>> input,
        ResponseContext responseContext
    )
    {
      Query<Result<DataSourceMetadataResultValue>> query = input.getQuery();
      if (!(query instanceof DataSourceMetadataQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass().getName(), DataSourceMetadataQuery.class);
      }

      final DataSourceMetadataQuery legacyQuery = (DataSourceMetadataQuery) query;

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
