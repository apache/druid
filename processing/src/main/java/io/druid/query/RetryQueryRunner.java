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
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;
import com.metamx.emitter.EmittingLogger;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.segment.SegmentMissingException;

import java.util.List;
import java.util.Map;

public class RetryQueryRunner<T> implements QueryRunner<T>
{
  public static String MISSING_SEGMENTS_KEY = "missingSegments";
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
  public Sequence<T> run(final Query<T> query, final Map<String, Object> context)
  {
    final List<Sequence<T>> listOfSequences = Lists.newArrayList();
    listOfSequences.add(baseRunner.run(query, context));

    return new YieldingSequenceBase<T>()
    {
      @Override
      public <OutType> Yielder<OutType> toYielder(
          OutType initValue, YieldingAccumulator<OutType, T> accumulator
      )
      {
        final List<SegmentDescriptor> missingSegments = getMissingSegments(context);

        if (!missingSegments.isEmpty()) {
          for (int i = 0; i < config.numTries(); i++) {
            log.info("[%,d] missing segments found. Retry attempt [%,d]", missingSegments.size(), i);

            context.put(MISSING_SEGMENTS_KEY, Lists.newArrayList());
            final Query<T> retryQuery = query.withQuerySegmentSpec(
                new MultipleSpecificSegmentSpec(
                    missingSegments
                )
            );
            Sequence<T> retrySequence = baseRunner.run(retryQuery, context);
            listOfSequences.add(retrySequence);
            if (getMissingSegments(context).isEmpty()) {
              break;
            }
          }

          final List<SegmentDescriptor> finalMissingSegs = getMissingSegments(context);
          if (!finalMissingSegs.isEmpty()) {
            throw new SegmentMissingException("No results found for segments[%s]", finalMissingSegs);
          }
        }

        return toolChest.mergeSequencesUnordered(Sequences.simple(listOfSequences)).toYielder(initValue, accumulator);
      }
    };
  }

  private List<SegmentDescriptor> getMissingSegments(final Map<String, Object> context)
  {
    final Object maybeMissingSegments = context.get(MISSING_SEGMENTS_KEY);
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

