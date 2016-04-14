package io.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.CPUTimeMetricQueryRunner;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.UnionQueryRunner;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FluentQueryRunnerBuilder<T>
{
  final QueryToolChest<T, Query<T>> toolChest;
  final Query<T> query;

  public FluentQueryRunner create(QueryRunner<T> baseRunner) {
    return new FluentQueryRunner(baseRunner);
  }

  public FluentQueryRunnerBuilder(QueryToolChest<T, Query<T>> toolChest, Query<T> query)
  {
    this.toolChest = toolChest;
    this.query = query;
  }

  public class FluentQueryRunner implements QueryRunner<T>
  {
    private QueryRunner<T> baseRunner;

    public FluentQueryRunner(QueryRunner<T> runner)
    {
      this.baseRunner = runner;
    }

    @Override
    public Sequence<T> run(
        Query<T> query, Map<String, Object> responseContext
    )
    {
      return baseRunner.run(query, responseContext);
    }

    public FluentQueryRunner from(QueryRunner<T> runner) {
      return new FluentQueryRunner(runner);
    }

    public FluentQueryRunner applyPostMergeDecoration()
    {
      return from(
          new FinalizeResultsQueryRunner<T>(
              toolChest.postMergeQueryDecoration(
                  baseRunner
              ),
              toolChest
          )
      );
    }

    public FluentQueryRunner applyPreMergeDecoration()
    {
      return from(
          new UnionQueryRunner<T>(
              toolChest.preMergeQueryDecoration(
                  baseRunner
              )
          )
      );
    }

    public FluentQueryRunner emitCPUTimeMetric(ServiceEmitter emitter)
    {
      return from(
          CPUTimeMetricQueryRunner.safeBuild(
              baseRunner,
              new Function<Query<T>, ServiceMetricEvent.Builder>()
              {
                @Nullable
                @Override
                public ServiceMetricEvent.Builder apply(Query<T> tQuery)
                {
                  return toolChest.makeMetricBuilder(tQuery);
                }
              },
              emitter,
              new AtomicLong(0L),
              true
          )
      );
    }

    public FluentQueryRunner postProcess(ObjectMapper objectMapper)
    {
      PostProcessingOperator<T> postProcessing = objectMapper.convertValue(
          query.<String>getContextValue("postProcessing"),
          new TypeReference<PostProcessingOperator<T>>()
          {
          }
      );

      return from(
          postProcessing != null ?
             postProcessing.postProcess(baseRunner) : baseRunner
      );
    }

    public FluentQueryRunner mergeResults()
    {
      return from(
          toolChest.mergeResults(baseRunner)
      );
    }
  }
}
