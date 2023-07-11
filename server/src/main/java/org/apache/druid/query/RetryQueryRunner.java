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
import org.apache.druid.query.context.ResponseContext.Keys;
import org.apache.druid.segment.SegmentMissingException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class RetryQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger LOG = new Logger(RetryQueryRunner.class);

  private final QueryRunner<T> baseRunner;
  private final BiFunction<Query<T>, List<SegmentDescriptor>, QueryRunner<T>> retryRunnerCreateFn;
  private final RetryQueryRunnerConfig config;
  private final ObjectMapper jsonMapper;

  /**
   * Runnable executed after the broker creates the query distribution tree for the first attempt. This is only
   * for testing and must not be used in production code.
   */
  private final Runnable runnableAfterFirstAttempt;

  private int totalNumRetries;

  public RetryQueryRunner(
      QueryRunner<T> baseRunner,
      BiFunction<Query<T>, List<SegmentDescriptor>, QueryRunner<T>> retryRunnerCreateFn,
      RetryQueryRunnerConfig config,
      ObjectMapper jsonMapper
  )
  {
    this(baseRunner, retryRunnerCreateFn, config, jsonMapper, () -> {});
  }

  /**
   * Constructor only for testing.
   */
  @VisibleForTesting
  RetryQueryRunner(
      QueryRunner<T> baseRunner,
      BiFunction<Query<T>, List<SegmentDescriptor>, QueryRunner<T>> retryRunnerCreateFn,
      RetryQueryRunnerConfig config,
      ObjectMapper jsonMapper,
      Runnable runnableAfterFirstAttempt
  )
  {
    this.baseRunner = baseRunner;
    this.retryRunnerCreateFn = retryRunnerCreateFn;
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.runnableAfterFirstAttempt = runnableAfterFirstAttempt;
  }

  @VisibleForTesting
  int getTotalNumRetries()
  {
    return totalNumRetries;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext context)
  {
    // Calling baseRunner.run() (which is SpecificQueryRunnable.run()) in the RetryingSequenceIterator
    // could be better because we can minimize the chance that data servers report missing segments as
    // we construct the query distribution tree when the query processing is actually started.
    // However, we call baseRunner.run() here instead where it's executed while constructing a query plan.
    // This is because ResultLevelCachingQueryRunner requires to compute the cache key based on
    // the segments to query which is computed in SpecificQueryRunnable.run().
    final Sequence<T> baseSequence = baseRunner.run(queryPlus, context);
    // runnableAfterFirstAttempt is only for testing, it must be no-op for production code.
    runnableAfterFirstAttempt.run();

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
                totalNumRetries = iterFromMake.retryCount;
              }
            }
        );
        return new MergeSequence<>(queryPlus.getQuery().getResultOrdering(), retryingSequence)
            .toYielder(initValue, accumulator);
      }
    };
  }

  private List<SegmentDescriptor> getMissingSegments(QueryPlus<T> queryPlus, final ResponseContext context)
  {
    // Sanity check before retrieving missingSegments from responseContext.
    // The missingSegments in the responseContext is only valid when all servers have responded to the broker.
    // The remainingResponses MUST be not null but 0 in the responseContext at this point.
    final ConcurrentHashMap<String, Integer> idToRemainingResponses =
        Preconditions.checkNotNull(
            context.getRemainingResponses(),
            "%s in responseContext",
            Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS.getName()
        );

    final int remainingResponses = Preconditions.checkNotNull(
        idToRemainingResponses.get(queryPlus.getQuery().getMostSpecificId()),
        "Number of remaining responses for query[%s]",
        queryPlus.getQuery().getMostSpecificId()
    );
    if (remainingResponses > 0) {
      throw new ISE("Failed to check missing segments due to missing responses from [%d] servers", remainingResponses);
    }

    // TODO: the sender's response may contain a truncated list of missing segments.
    // Truncation is aggregated in the response context given as a parameter.
    // Check the getTruncated() value: if true, then the we don't know the full set of
    // missing segments.
    final List<SegmentDescriptor> maybeMissingSegments = context.getMissingSegments();
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
   * A lazy iterator populating a {@link Sequence} by retrying the query. The first returned sequence is always the base
   * sequence from the baseQueryRunner. Subsequent sequences are created dynamically whenever it retries the query. All
   * the sequences populated by this iterator will be merged (not combined) with the base sequence.
   *
   * The design of this iterator depends on how {@link MergeSequence} works; the MergeSequence pops an item from
   * each underlying sequence and pushes them to a {@link java.util.PriorityQueue}. Whenever it pops from the queue,
   * it pushes a new item from the sequence where the returned item was originally from. Since the first returned
   * sequence from this iterator is always the base sequence, the MergeSequence will call {@link Sequence#toYielder}
   * on the base sequence first which in turn initializes the query distribution tree. Once this tree is built, the query
   * servers (historicals and realtime tasks) will lock all segments to read and report missing segments to the broker.
   * If there are missing segments reported, this iterator will rewrite the query with those reported segments and
   * reissue the rewritten query.
   *
   * @see org.apache.druid.client.CachingClusteredClient
   * @see org.apache.druid.client.DirectDruidClient
   */
  private class RetryingSequenceIterator implements Iterator<Sequence<T>>
  {
    private final QueryPlus<T> queryPlus;
    private final ResponseContext context;

    private Sequence<T> sequence;
    private int retryCount = 0;

    private RetryingSequenceIterator(
        QueryPlus<T> queryPlus,
        ResponseContext context,
        Sequence<T> baseSequence
    )
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
        final QueryContext queryContext = queryPlus.getQuery().context();
        final List<SegmentDescriptor> missingSegments = getMissingSegments(queryPlus, context);
        final int maxNumRetries = queryContext.getNumRetriesOnMissingSegments(
            config.getNumTries()
        );
        if (missingSegments.isEmpty()) {
          return false;
        } else if (retryCount >= maxNumRetries) {
          if (!queryContext.allowReturnPartialResults(config.isReturnPartialResults())) {
            throw new SegmentMissingException("No results found for segments[%s]", missingSegments);
          } else {
            return false;
          }
        } else {
          retryCount++;
          LOG.info("[%,d] missing segments found. Retry attempt [%,d]", missingSegments.size(), retryCount);

          context.initializeMissingSegments();
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
