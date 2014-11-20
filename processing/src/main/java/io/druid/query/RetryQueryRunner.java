/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;
import com.metamx.emitter.EmittingLogger;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.segment.SegmentMissingException;
import org.joda.time.Interval;

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
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final List<Sequence<T>> listOfSequences = Lists.newArrayList();
    listOfSequences.add(baseRunner.run(query, responseContext));

    return new YieldingSequenceBase<T>()
    {
      @Override
      public <OutType> Yielder<OutType> toYielder(
          OutType initValue, YieldingAccumulator<OutType, T> accumulator
      )
      {
        // Try to find missing segments
        doRetryLogic(
            responseContext,
            Result.MISSING_SEGMENTS_KEY,
            new TypeReference<List<SegmentDescriptor>>()
            {
            },
            new Function<List<SegmentDescriptor>, Query<T>>()
            {
              @Override
              public Query<T> apply(List<SegmentDescriptor> input)
              {
                return query.withQuerySegmentSpec(
                    new MultipleSpecificSegmentSpec(
                        input
                    )
                );
              }
            },
            listOfSequences
        );

        // Try to find missing intervals
        doRetryLogic(
            responseContext,
            Result.MISSING_INTERVALS_KEY,
            new TypeReference<List<Interval>>()
            {
            },
            new Function<List<Interval>, Query<T>>()
            {
              @Override
              public Query<T> apply(List<Interval> input)
              {
                return query.withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(
                        input
                    )
                );
              }
            },
            listOfSequences
        );

        return toolChest.mergeSequencesUnordered(Sequences.simple(listOfSequences)).toYielder(initValue, accumulator);
      }
    };
  }

  private <Type> void doRetryLogic(
      final Map<String, Object> responseContext,
      final String key,
      final TypeReference<List<Type>> typeReference,
      final Function<List<Type>, Query<T>> function,
      final List<Sequence<T>> listOfSequences
  )
  {
    final List<Type> missingItems = getMissingItems(responseContext, key, typeReference);

    if (!missingItems.isEmpty()) {
      for (int i = 0; i < config.getNumTries(); i++) {
        log.info("[%,d] missing items found. Retry attempt [%,d]", missingItems.size(), i);

        responseContext.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
        final Query<T> retryQuery = function.apply(missingItems);
        Sequence<T> retrySequence = baseRunner.run(retryQuery, responseContext);
        listOfSequences.add(retrySequence);
        if (getMissingItems(responseContext, key, typeReference).isEmpty()) {
          break;
        }
      }

      final List<Type> finalMissingItems = getMissingItems(responseContext, key, typeReference);
      if (!config.isReturnPartialResults() && !finalMissingItems.isEmpty()) {
        throw new SegmentMissingException("No results found for items[%s]", finalMissingItems);
      }
    }
  }

  private <Type> List<Type> getMissingItems(
      final Map<String, Object> context,
      final String key,
      final TypeReference<List<Type>> typeReference
  )
  {
    final Object maybeMissing = context.get(key);
    if (maybeMissing == null) {
      return Lists.newArrayList();
    }

    return jsonMapper.convertValue(
        maybeMissing,
        typeReference
    );
  }
}

