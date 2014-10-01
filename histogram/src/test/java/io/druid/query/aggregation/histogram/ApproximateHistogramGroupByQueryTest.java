package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.LegacyDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.TestHelper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class ApproximateHistogramGroupByQueryTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;
  private Supplier<GroupByQueryConfig> configSupplier;

  @Before
  public void setUp() throws Exception
  {
    configSupplier = Suppliers.ofInstance(new GroupByQueryConfig());
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final StupidPool<ByteBuffer> pool = new StupidPool<ByteBuffer>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        configSupplier,
        new GroupByQueryQueryToolChest(configSupplier, mapper, engine)
    );

    GroupByQueryConfig singleThreadedConfig = new GroupByQueryConfig()
    {
      @Override
      public boolean isSingleThreaded()
      {
        return true;
      }
    };
    singleThreadedConfig.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> singleThreadedConfigSupplier = Suppliers.ofInstance(singleThreadedConfig);
    final GroupByQueryEngine singleThreadEngine = new GroupByQueryEngine(singleThreadedConfigSupplier, pool);

    final GroupByQueryRunnerFactory singleThreadFactory = new GroupByQueryRunnerFactory(
        singleThreadEngine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        singleThreadedConfigSupplier,
        new GroupByQueryQueryToolChest(singleThreadedConfigSupplier, mapper, singleThreadEngine)
    );


    Function<Object, Object> function = new Function<Object, Object>()
    {
      @Override
      public Object apply(@Nullable Object input)
      {
        return new Object[]{factory, ((Object[]) input)[0]};
      }
    };

    return Lists.newArrayList(
        Iterables.concat(
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(factory),
                function
            ),
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(singleThreadFactory),
                function
            )
        )
    );
  }

  public ApproximateHistogramGroupByQueryTest(GroupByQueryRunnerFactory factory, QueryRunner runner)
  {
    this.factory = factory;
    this.runner = runner;
  }

  @Test
  public void testGroupByNWithApproximateHistogramAgg()
  {
    ApproximateHistogramAggregatorFactory aggFactory = new ApproximateHistogramAggregatorFactory(
        "apphisto",
        "index",
        10,
        5,
        Float.NEGATIVE_INFINITY,
        Float.POSITIVE_INFINITY
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(Arrays.<DimensionSpec>asList(new LegacyDimensionSpec(QueryRunnerTestHelper.providerDimension)))
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setAggregatorSpecs(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index"),
                        aggFactory
                    )
                )
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg,
                new QuantilePostAggregator("quantile", "apphisto", 0.5f)
            )
        )
        .build();

    List<Row> expectedResults = Arrays.<Row>asList(
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "approx-histo");
  }
}
