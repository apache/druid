package io.druid.query;

import com.google.common.base.Supplier;
import io.druid.collections.StupidPool;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchQueryRunnerFactory;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.segment.Segment;

import java.nio.ByteBuffer;

/**
 */
public class TestQueryRunners
{
  public static final StupidPool<ByteBuffer> pool = new StupidPool<ByteBuffer>(
      new Supplier<ByteBuffer>()
      {
        @Override
        public ByteBuffer get()
        {
          return ByteBuffer.allocate(1024 * 10);
        }
      }
  );

  public static final TopNQueryConfig topNConfig = new TopNQueryConfig();

  public static StupidPool<ByteBuffer> getPool()
  {
    return pool;
  }

  public static <T> QueryRunner<T> makeTopNQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        pool,
        new TopNQueryQueryToolChest(topNConfig),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }

  public static <T> QueryRunner<T> makeTimeSeriesQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(new QueryConfig()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }

  public static <T> QueryRunner<T> makeSearchQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new SearchQueryRunnerFactory(new SearchQueryQueryToolChest(new SearchQueryConfig()), QueryRunnerTestHelper.NOOP_QUERYWATCHER);
    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }

  public static <T> QueryRunner<T> makeTimeBoundaryQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new TimeBoundaryQueryRunnerFactory(QueryRunnerTestHelper.NOOP_QUERYWATCHER);
    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }
}
