package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RetryQueryRunnerTest
{

  final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                .dataSource(QueryRunnerTestHelper.dataSource)
                                .granularity(QueryRunnerTestHelper.dayGran)
                                .intervals(QueryRunnerTestHelper.firstToThird)
                                .aggregators(
                                    Arrays.<AggregatorFactory>asList(
                                        QueryRunnerTestHelper.rowsCount,
                                        new LongSumAggregatorFactory(
                                            "idx",
                                            "index"
                                        ),
                                        QueryRunnerTestHelper.qualityUniques
                                    )
                                )
                                .build();


  @Test
  public void testRunWithMissingSegments() throws Exception
  {
    Map<String, Object> context = new MapMaker().makeMap();
    context.put("missingSegments", Lists.newArrayList());
    RetryQueryRunner runner = new RetryQueryRunner(
        new QueryRunner()
        {
          @Override
          public Sequence run(Query query, Map context)
          {
            ((List)context.get(RetryQueryRunner.missingSegments)).add(new SegmentDescriptor(new Interval(178888, 1999999), "test", 1));
            return Sequences.empty();
          }
        },
        new QueryToolChest()
        {
          @Override
          public QueryRunner mergeResults(QueryRunner runner)
          {
            return null;
          }

          @Override
          public Sequence mergeSequences(Sequence seqOfSequences)
          {
            return new OrderedMergeSequence<Result<TimeseriesResultValue>>(getOrdering(), seqOfSequences);
          }

          @Override
          public ServiceMetricEvent.Builder makeMetricBuilder(Query query)
          {
            return null;
          }

          @Override
          public Function makePreComputeManipulatorFn(
              Query query, MetricManipulationFn fn
          )
          {
            return null;
          }

          @Override
          public TypeReference getResultTypeReference()
          {
            return null;
          }

          public Ordering<Result<TimeseriesResultValue>> getOrdering()
          {
            return Ordering.natural();
          }
        },
        new RetryQueryRunnerConfig()
        {
          private int numTries = 1;
          private boolean returnPartialResults = true;

          public int numTries() { return numTries; }
          public boolean returnPartialResults() { return returnPartialResults; }
        }
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    Assert.assertTrue("Should return an empty sequence as a result", ((List) actualResults).size() == 0);
  }
}