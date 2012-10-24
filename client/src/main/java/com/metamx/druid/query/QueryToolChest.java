package com.metamx.druid.query;

import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.codehaus.jackson.type.TypeReference;

/**
 * The broker-side (also used by server in some cases) API for a specific Query type.  This API is still undergoing
 * evolution and is only semi-stable, so proprietary Query implementations should be ready for the potential
 * maintenance burden when upgrading versions.
 */
public interface QueryToolChest<ResultType, QueryType extends Query<ResultType>>
{
  public QueryRunner<ResultType> mergeResults(QueryRunner<ResultType> runner);

  /**
   * This method doesn't belong here, but it's here for now just to make it work.
   *
   * @param seqOfSequences
   * @return
   */
  public Sequence<ResultType> mergeSequences(Sequence<Sequence<ResultType>> seqOfSequences);
  public ServiceMetricEvent.Builder makeMetricBuilder(QueryType query);
  public Function<ResultType, ResultType> makeMetricManipulatorFn(QueryType query, MetricManipulationFn fn);
  public TypeReference<ResultType> getResultTypeReference();
  public CacheStrategy<ResultType, QueryType> getCacheStrategy(QueryType query);
  public QueryRunner<ResultType> preMergeQueryDecoration(QueryRunner<ResultType> runner);
  public QueryRunner<ResultType> postMergeQueryDecoration(QueryRunner<ResultType> runner);
}
