/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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
  public <T> CacheStrategy<ResultType, T, QueryType> getCacheStrategy(QueryType query);
  public QueryRunner<ResultType> preMergeQueryDecoration(QueryRunner<ResultType> runner);
  public QueryRunner<ResultType> postMergeQueryDecoration(QueryRunner<ResultType> runner);
}
