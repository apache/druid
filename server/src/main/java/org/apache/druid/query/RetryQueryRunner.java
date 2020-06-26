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

package org.apache.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.guava.YieldingSequenceBase;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.segment.SegmentMissingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;

public class RetryQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger LOG = new Logger(RetryQueryRunner.class);

  private final QueryRunner<T> baseRunner;
  private final BiFunction<Query<T>, List<SegmentDescriptor>, QueryRunner<T>> retryRunnerCreateFn;
  private final RetryQueryRunnerConfig config;
  private final ObjectMapper jsonMapper;

  private int numTotalRetries;

  public RetryQueryRunner(
      QueryRunner<T> baseRunner,
      BiFunction<Query<T>, List<SegmentDescriptor>, QueryRunner<T>> retryRunnerCreateFn,
      RetryQueryRunnerConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.baseRunner = baseRunner;
    this.retryRunnerCreateFn = retryRunnerCreateFn;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @VisibleForTesting
  int getNumTotalRetries()
  {
    return numTotalRetries;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext context)
  {
    final Sequence<T> baseSequence = baseRunner.run(queryPlus, context);
    return new YieldingSequenceBase<T>()
    {
      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        final Sequence<Sequence<T>> retryingSequence = new BaseSequence<>(
            new IteratorMaker<Sequence<T>, RetryingSequenceIterator>()
            {
              @Override
              public RetryingSequenceIterator make()
              {
                return new RetryingSequenceIterator(queryPlus, context, baseSequence);
              }

              @Override
              public void cleanup(RetryingSequenceIterator iterFromMake)
              {
                numTotalRetries = iterFromMake.retryCount;
              }
            }
        );
        return new MergeSequence<>(queryPlus.getQuery().getResultOrdering(), retryingSequence)
            .toYielder(initValue, accumulator);
      }
    };
  }

  private List<SegmentDescriptor> getMissingSegments(final ResponseContext context)
  {
    // Sanity check before retrieving missingSegments from responseContext.
    // The missingSegments in the responseContext is only valid when all servers have responded to the broker.
    // The remainingResponses must be not null but 0 in the responseContext at this point.
    final int remainingResponses = Preconditions.checkNotNull(
        (Integer) context.get(Key.REMAINING_RESPONSES_FROM_QUERY_NODES),
        "%s in responseContext",
        Key.REMAINING_RESPONSES_FROM_QUERY_NODES.getName()
    );
    if (remainingResponses > 0) {
      throw new ISE("Failed to check missing segments due to missing responds from [%d] servers", remainingResponses);
    }

    final Object maybeMissingSegments = context.get(ResponseContext.Key.MISSING_SEGMENTS);
    if (maybeMissingSegments == null) {
      return Collections.emptyList();
    }

    return jsonMapper.convertValue(
        maybeMissingSegments,
        new TypeReference<List<SegmentDescriptor>>()
        {
        }
    );
  }

  /**
   * A lazy iterator populating {@link Sequence} by retrying the query. The first returned sequence is always the base
   * sequence given in the constructor. Subsequent sequences are created dynamically whenever it retries the query.
   */
  private class RetryingSequenceIterator implements Iterator<Sequence<T>>
  {
    private final QueryPlus<T> queryPlus;
    private final ResponseContext context;
    private Sequence<T> sequence;
    private int retryCount = 0;

    private RetryingSequenceIterator(QueryPlus<T> queryPlus, ResponseContext context, Sequence<T> baseSequence)
    {
      this.queryPlus = queryPlus;
      this.context = context;
      this.sequence = baseSequence;
    }

    @Override
    public boolean hasNext()
    {
      if (sequence != null) {
        return true;
      } else {
        final List<SegmentDescriptor> missingSegments = getMissingSegments(context);
        if (missingSegments.isEmpty()) {
          return false;
        } else if (retryCount >= config.getNumTries()) {
          if (!config.isReturnPartialResults()) {
            throw new SegmentMissingException("No results found for segments[%s]", missingSegments);
          } else {
            return false;
          }
        } else {
          LOG.info("[%,d] missing segments found. Retry attempt [%,d]", missingSegments.size(), retryCount++);

          context.put(ResponseContext.Key.MISSING_SEGMENTS, new ArrayList<>());
          final QueryPlus<T> retryQueryPlus = queryPlus.withQuery(
              Queries.withSpecificSegments(queryPlus.getQuery(), missingSegments)
          );
          sequence = retryRunnerCreateFn.apply(retryQueryPlus.getQuery(), missingSegments).run(retryQueryPlus, context);
          return true;
        }
      }
    }

    @Override
    public Sequence<T> next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final Sequence<T> next = sequence;
      sequence = null;
      return next;
    }
  }
}
