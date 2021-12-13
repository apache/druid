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

package org.apache.druid.server;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Wraps the two results from a query: the results and the response context.
 */
public class QueryResponse<T>
{
  private final Sequence<T> results;
  private final ResponseContext responseContext;
  private final AtomicBoolean resultsComplete;

  private QueryResponse(
      final Sequence<T> results,
      final ResponseContext responseContext,
      final AtomicBoolean resultsComplete
  )
  {
    this.results = results;
    this.responseContext = responseContext;
    this.resultsComplete = resultsComplete;
  }

  public static <T> QueryResponse<T> create(final Sequence<T> results, final ResponseContext responseContext)
  {
    final AtomicBoolean resultsComplete = new AtomicBoolean();

    return new QueryResponse<>(
        Sequences.wrap(
            results,
            new SequenceWrapper()
            {
              @Override
              public void after(boolean isDone, Throwable thrown)
              {
                resultsComplete.compareAndSet(false, isDone);
              }
            }
        ),
        responseContext,
        resultsComplete
    );
  }


  public static <T> QueryResponse<T> createWithEmptyContext(final Sequence<T> results)
  {
    return create(results, ConcurrentResponseContext.createEmpty());
  }

  /**
   * Returns the results.
   */
  public Sequence<T> getResults()
  {
    return results;
  }

  /**
   * Returns the response context. Will only be present after the results have been successfully and fully walked.
   */
  public Optional<ResponseContext> getResponseContext()
  {
    if (resultsComplete.get()) {
      return Optional.of(responseContext);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Returns the response context. Will be present even before results have been successfully and fully walked.
   * It may still be undergoing mutation, so it is up to you, the caller, to only access fields with thread-safe values.
   *
   * In most cases it is safer to use {@link #getResponseContext()}.
   */
  public ResponseContext getResponseContextEarly()
  {
    return responseContext;
  }

  public <U> QueryResponse<U> map(final Function<Sequence<T>, Sequence<U>> mapper)
  {
    return new QueryResponse<>(mapper.apply(results), responseContext, resultsComplete);
  }

  public QueryResponse<T> wrap(final SequenceWrapper wrapper)
  {
    return new QueryResponse<>(Sequences.wrap(results, wrapper), responseContext, resultsComplete);
  }

  /**
   * Create a new response with new (presumably rewritten) results.
   */
  public QueryResponse<T> rewrite(Sequence<T> newResults)
  {
    return new QueryResponse<>(newResults, responseContext, resultsComplete);
  }
}
