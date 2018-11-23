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

package org.apache.druid.java.util.common.guava;

import com.google.common.base.Supplier;

/**
 * @see Sequences#wrap(Sequence, SequenceWrapper)
 */
public abstract class SequenceWrapper
{
  /**
   * Executed before sequence processing, i. e. before {@link Sequence#accumulate} or {@link Sequence#toYielder} on the
   * wrapped sequence. Default implementation does nothing.
   */
  public void before()
  {
    // do nothing
  }

  /**
   * Wraps any bits of the wrapped sequence processing: {@link Sequence#accumulate} or {@link Sequence#toYielder} and
   * {@link Yielder#next(Object)} on the wrapped yielder. Doesn't wrap {@link #before} and {@link #after}.
   *
   * <p>{@code sequenceProcessing.get()} must be called just once. Implementation of this method should look like
   * <pre>
   * ... do something
   * try {
   *   return sequenceProcessing.get();
   * }
   * finally {
   *   ... do something else
   * }
   * </pre>
   */
  public <RetType> RetType wrap(Supplier<RetType> sequenceProcessing)
  {
    return sequenceProcessing.get();
  }

  /**
   * Executed after sequence processing, i. e. after {@link Sequence#accumulate} on the wrapped sequence or after {@link
   * Yielder#close()} on the wrapped yielder, or if exception was thrown from any method called on the wrapped sequence
   * or yielder, or from {@link #before}, or from {@link #wrap} methods of this SequenceWrapper.
   *
   * <p>Even if {@code thrown} is not null, implementation of this method shouldn't rethrow it, it is done outside.
   *
   * @param isDone true if all elements in the sequence were processed and no exception was thrown, false otherwise
   * @param thrown an exception thrown from any method called on the wrapped sequence or yielder, or from {@link
   * #before()}, or from {@link #wrap} methods of this SequenceWrapper, or null if no exception was thrown.
   */
  public void after(boolean isDone, Throwable thrown) throws Exception
  {
    // do nothing
  }
}
