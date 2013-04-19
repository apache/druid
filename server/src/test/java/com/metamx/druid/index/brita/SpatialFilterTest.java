package com.metamx.druid.index.brita;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.spatial.rtree.search.RadiusBound;
import com.metamx.druid.Druids;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.TestHelper;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.CountAggregatorFactory;
import com.metamx.druid.aggregation.DoubleSumAggregatorFactory;
import com.metamx.druid.aggregation.LongSumAggregatorFactory;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.QueryableIndexSegment;
import com.metamx.druid.index.v1.IncrementalIndex;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.index.v1.IndexMerger;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.query.FinalizeResultsQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.filter.SpatialDimFilter;
import com.metamx.druid.query.timeseries.TimeseriesQuery;
import com.metamx.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeseriesResultValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
public class SpatialFilterTest
{
  private QueryableIndex makeIndex() throws IOException
  {
    final List<String> dims = Lists.newArrayList("dim", "dim.geo");
    IncrementalIndex theIndex = new IncrementalIndex(
        new DateTime("2013-01-01").getMillis(),
        QueryGranularity.DAY,
        new AggregatorFactory[]{
            new CountAggregatorFactory("rows"),
            new LongSumAggregatorFactory("val", "val")
        }
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-01").getMillis(),
            dims,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-01").toString(),
                "dim", "foo",
                "dim.geo", Arrays.asList(0.0f, 0.0f),
                "val", 17l
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-02").getMillis(),
            dims,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-02").toString(),
                "dim", "foo",
                "dim.geo", Arrays.asList(1.0f, 3.0f),
                "val", 29l
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-03").getMillis(),
            dims,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-03").toString(),
                "dim", "foo",
                "dim.geo", Arrays.asList(4.0f, 2.0f),
                "val", 13l
            )
        )
    );
    theIndex.add(
        new MapBasedInputRow(
            new DateTime("2013-01-03").getMillis(),
            dims,
            ImmutableMap.<String, Object>of(
                "timestamp", new DateTime("2013-01-03").toString(),
                "dim", "foo",
                "dim.geo", Arrays.asList(7.0f, 3.0f),
                "val", 91l
            )
        )
    );

    File tmpFile = File.createTempFile("billy", "yay");
    tmpFile.delete();
    tmpFile.mkdirs();
    tmpFile.deleteOnExit();

    IndexMerger.persist(theIndex, tmpFile);
    return IndexIO.loadIndex(tmpFile);
  }

  @Test
  public void testSpatialQuery()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity(QueryGranularity.ALL)
                                  .intervals(Arrays.asList(new Interval("2013-01-01/2013-01-07")))
                                  .filters(new SpatialDimFilter("dim.geo", new RadiusBound(new float[]{0.0f, 0.0f}, 5)))
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          new CountAggregatorFactory("rows"),
                                          new LongSumAggregatorFactory("val", "val")
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2013-01-01T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 3L)
                            .put("val", 59l)
                            .build()
            )
        )
    );
    try {
      TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory();
      QueryRunner runner = new FinalizeResultsQueryRunner(
          factory.createRunner(new QueryableIndexSegment(null, makeIndex())),
          factory.getToolchest()
      );

      TestHelper.assertExpectedResults(expectedResults, runner.run(query));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
