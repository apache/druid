/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.queryng.operators;

import com.google.common.base.Function;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.queryng.fragment.FragmentContext;

/**
 * Operator that applies a function to each input item to produce the output item.
 *
 * Generalization of {@link QueryToolChest#makePostComputeManipulatorFn(Query, MetricManipulationFn)} to the
 * result stream. When used in this role, the operator is expected to be the operator in the pipeline,
 * after results are fully merged.
 * <p>
 * Note that, when used in the above role,  despite the type parameter "T", this runner may not actually
 * return sequences with type T. This most
 * commonly happens when an upstream {@link BySegmentQueryRunner} changes the result stream to type
 * {@code Result<BySegmentResultValue<T>>}, in which case this class will retain the structure, but call the finalizer
 * function on each result in the by-segment list (which may change their type from T to something else).
 *
 * @see {@link org.apache.druid.query.FinalizeResultsQueryRunner}
 */
public class TransformOperator<IN, OUT> extends MappingOperator<IN, OUT>
{
  private final Function<IN, OUT> transformFn;

  public TransformOperator(
      FragmentContext context,
      final Function<IN, OUT> transformFn,
      final Operator<IN> input)
  {
    super(context, input);
    this.transformFn = transformFn;
  }

  @Override
  public OUT next()
  {
    return transformFn.apply(inputIter.next());
  }
}
