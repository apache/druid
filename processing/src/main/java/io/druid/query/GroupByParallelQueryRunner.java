package io.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.segment.incremental.IncrementalIndex;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


public class GroupByParallelQueryRunner implements QueryRunner<Row>
{
  private static final Logger log = new Logger(GroupByParallelQueryRunner.class);
  private final Iterable<QueryRunner<Row>> queryables;
  private final ExecutorService exec;
  private final Ordering<Row> ordering;
  private final Supplier<GroupByQueryConfig> configSupplier;

  public GroupByParallelQueryRunner(
      ExecutorService exec,
      Ordering<Row> ordering, Supplier<GroupByQueryConfig> configSupplier,
      QueryRunner<Row>... queryables
  )
  {
    this(exec, ordering, configSupplier, Arrays.asList(queryables));
  }

  public GroupByParallelQueryRunner(
      ExecutorService exec,
      Ordering<Row> ordering, Supplier<GroupByQueryConfig> configSupplier,
      Iterable<QueryRunner<Row>> queryables
  )
  {
    this.exec = exec;
    this.ordering = ordering;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.configSupplier = configSupplier;
  }

  @Override
  public Sequence<Row> run(final Query<Row> queryParam)
  {

    final GroupByQuery query = (GroupByQuery) queryParam;
    final Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> indexAccumulatorPair = GroupByQueryHelper.createIndexAccumulatorPair(
        query,
        configSupplier.get()
    );
    final int priority = Integer.parseInt(query.getContextValue("priority", "0"));

    if (Iterables.isEmpty(queryables)) {
      log.warn("No queryables found.");
    }
    List<Future<Boolean>> futures = Lists.newArrayList(
        Iterables.transform(
            queryables,
            new Function<QueryRunner<Row>, Future<Boolean>>()
            {
              @Override
              public Future<Boolean> apply(final QueryRunner<Row> input)
              {
                return exec.submit(
                    new PrioritizedCallable<Boolean>(priority)
                    {
                      @Override
                      public Boolean call() throws Exception
                      {
                        try {
                          input.run(queryParam).accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
                          return true;
                        }
                        catch (Exception e) {
                          log.error(e, "Exception with one of the sequences!");
                          throw Throwables.propagate(e);
                        }
                      }
                    }
                );
              }
            }
        )
    );

    // Let the runners complete
    for (Future<Boolean> future : futures) {
      try {
        future.get();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    return Sequences.simple(indexAccumulatorPair.lhs.iterableWithPostAggregations(null));
  }

}
