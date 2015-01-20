/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.datasourcemetadata;

import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
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
        queryExecutor, toolChest.getOrdering(), queryWatcher, queryRunners
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
