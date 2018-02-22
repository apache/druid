package io.druid.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.VirtualColumns;
import org.joda.time.DateTime;
import org.junit.Test;

public class RequestLogLineTest
{

  @Test
  public void testSimple() throws Exception {
    final RequestLogLine line = new RequestLogLine(
        DateTime.now(),
        "127.0.0.1",
        new TopNQuery(
            new TableDataSource("dummy"),
            VirtualColumns.EMPTY,
            new DefaultDimensionSpec("test", "test"),
            new NumericTopNMetricSpec("metric1"),
            3,
            new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
            null,
            Granularities.ALL,
            ImmutableList.<AggregatorFactory>of(new CountAggregatorFactory("metric1")),
            ImmutableList.<PostAggregator>of(new ConstantPostAggregator("post", 10)),
            null
        ),
        new QueryStats(
            ImmutableMap.of()
        )
    );
    final String print = line.getLine(new DefaultObjectMapper());
    System.out.println(print);
  }
}