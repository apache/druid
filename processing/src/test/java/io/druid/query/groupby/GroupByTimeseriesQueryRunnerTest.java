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

package io.druid.query.groupby;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryRunnerTest;
import io.druid.query.timeseries.TimeseriesResultValue;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

/**
 */
@RunWith(Parameterized.class)
public class GroupByTimeseriesQueryRunnerTest extends TimeseriesQueryRunnerTest
{
  @SuppressWarnings("unchecked")
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(
        configSupplier,
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        configSupplier,
        new GroupByQueryQueryToolChest(configSupplier, engine)
    );

    final Collection<?> objects = QueryRunnerTestHelper.makeQueryRunners(factory);
    Object[][] newObjects = new Object[objects.size()][];
    int i = 0;
    for (Object object : objects) {
      if (object instanceof Object[]) {
        Object[] queryRunnerArray = (Object[]) object;

        Preconditions.checkState(queryRunnerArray.length == 1);
        Preconditions.checkState(queryRunnerArray[0] instanceof QueryRunner);

        final QueryRunner groupByRunner = (QueryRunner) queryRunnerArray[0];
        QueryRunner timeseriesRunner = new QueryRunner()
        {
          @Override
          public Sequence run(Query query)
          {
            TimeseriesQuery tsQuery = (TimeseriesQuery) query;

            return Sequences.map(
                groupByRunner.run(
                    GroupByQuery.builder()
                        .setDataSource(tsQuery.getDataSource())
                        .setQuerySegmentSpec(tsQuery.getQuerySegmentSpec())
                        .setGranularity(tsQuery.getGranularity())
                        .setDimFilter(tsQuery.getDimensionsFilter())
                        .setAggregatorSpecs(tsQuery.getAggregatorSpecs())
                        .setPostAggregatorSpecs(tsQuery.getPostAggregatorSpecs())
                        .build()
                ),
                new Function<Row, Result<TimeseriesResultValue>>()
                {
                  @Override
                  public Result<TimeseriesResultValue> apply(final Row input)
                  {
                    MapBasedRow row = (MapBasedRow) input;

                    return new Result<TimeseriesResultValue>(
                        row.getTimestamp(), new TimeseriesResultValue(row.getEvent())
                    );
                  }
                }
            );
          }
        };

        newObjects[i] = new Object[]{timeseriesRunner};
        ++i;
      }
    }

    return Arrays.asList(newObjects);
  }

  public GroupByTimeseriesQueryRunnerTest(QueryRunner runner)
  {
    super(runner);
  }

  @Override
  public void testFullOnTimeseries()
  {
    // Skip this test because the timeseries test expects a skipped day to be filled in, but group by doesn't
    // fill anything in.
  }

  @Override
  public void testFullOnTimeseriesWithFilter()
  {
    // Skip this test because the timeseries test expects a skipped day to be filled in, but group by doesn't
    // fill anything in.
  }

  @Override
  public void testTimeseriesWithNonExistentFilter()
  {
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }

  @Override
  public void testTimeseriesWithNonExistentFilterAndMultiDim()
  {
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }

  @Override
  public void testTimeseriesWithFilterOnNonExistentDimension()
  {
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }
}
