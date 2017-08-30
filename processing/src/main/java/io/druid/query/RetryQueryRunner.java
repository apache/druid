/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.guava.YieldingSequenceBase;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.segment.SegmentMissingException;

import java.util.List;
import java.util.Map;

public class RetryQueryRunner<T> implements QueryRunner<T>
{
  private static final EmittingLogger log = new EmittingLogger(RetryQueryRunner.class);

  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;
  private final RetryQueryRunnerConfig config;
  private final ObjectMapper jsonMapper;

  public RetryQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest,
      RetryQueryRunnerConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final Map<String, Object> context)
  {
    final List<Sequence<T>> listOfSequences = Lists.newArrayList();
    listOfSequences.add(baseRunner.run(queryPlus, context));

    return new YieldingSequenceBase<T>()
    {
      @Override
      public <OutType> Yielder<OutType> toYielder(
          OutType initValue, YieldingAccumulator<OutType, T> accumulator
      )
      {
        List<SegmentDescriptor> missingSegments = getMissingSegments(context);

        if (!missingSegments.isEmpty()) {
          for (int i = 0; i < config.getNumTries(); i++) {
            log.info("[%,d] missing segments found. Retry attempt [%,d]", missingSegments.size(), i);

            context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
            final QueryPlus<T> retryQueryPlus = queryPlus.withQuerySegmentSpec(
                new MultipleSpecificSegmentSpec(
                    missingSegments
                )
            );
            Sequence<T> retrySequence = baseRunner.run(retryQueryPlus, context);
            listOfSequences.add(retrySequence);
            missingSegments = getMissingSegments(context);
            if (missingSegments.isEmpty()) {
              break;
            }
          }

          final List<SegmentDescriptor> finalMissingSegs = getMissingSegments(context);
          if (!config.isReturnPartialResults() && !finalMissingSegs.isEmpty()) {
            throw new SegmentMissingException("No results found for segments[%s]", finalMissingSegs);
          }

          return new MergeSequence<>(
              queryPlus.getQuery().getResultOrdering(),
              Sequences.simple(listOfSequences)).toYielder(
              initValue, accumulator
          );
        } else {
          return Iterables.getOnlyElement(listOfSequences).toYielder(initValue, accumulator);
        }
      }
    };
  }

  private List<SegmentDescriptor> getMissingSegments(final Map<String, Object> context)
  {
    final Object maybeMissingSegments = context.get(Result.MISSING_SEGMENTS_KEY);
    if (maybeMissingSegments == null) {
      return Lists.newArrayList();
    }

    return jsonMapper.convertValue(
        maybeMissingSegments,
        new TypeReference<List<SegmentDescriptor>>()
        {
        }
    );
  }
}
