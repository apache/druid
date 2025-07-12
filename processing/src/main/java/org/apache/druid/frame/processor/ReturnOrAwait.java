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

package org.apache.druid.frame.processor;

import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Instances of this class are returned by {@link FrameProcessor#runIncrementally}, and are used by
 * {@link FrameProcessorExecutor} to manage execution.
 *
 * An instance can be a "return" with a result, which means that the {@link FrameProcessor} is done working.
 * In this case {@link #isReturn()} is true and {@link #value()} contains the result.
 *
 * An instance can also be an "await", which means that the {@link FrameProcessor} wants to be scheduled again
 * in the future. In this case {@link #isAwait()} is true, and *either* {@link #hasAwaitableChannels()} or
 * {@link #hasAwaitableFutures()} is true.
 */
public class ReturnOrAwait<T>
{
  private static final IntSet RANGE_SET_ONE = IntSets.singleton(0);

  @Nullable
  private final T retVal;

  @Nullable
  private final IntSet awaitChannels;

  private final boolean awaitAllChannels;

  @Nullable
  private final Collection<ListenableFuture<?>> awaitFutures;

  private ReturnOrAwait(
      @Nullable T retVal,
      @Nullable IntSet awaitChannels,
      @Nullable Collection<ListenableFuture<?>> awaitFutures,
      final boolean awaitAllChannels
  )
  {
    this.retVal = retVal;
    this.awaitChannels = awaitChannels;
    this.awaitAllChannels = awaitAllChannels;
    this.awaitFutures = awaitFutures;

    if (retVal != null && (awaitChannels != null || awaitFutures != null)) {
      throw new IAE("Cannot have a value when await != null or futures != null");
    }

    if (awaitChannels != null && awaitFutures != null) {
      throw new ISE("Cannot have both awaitChannels and awaitFutures");
    }
  }

  /**
   * Wait for nothing; that is: run again as soon as possible.
   */
  public static <T> ReturnOrAwait<T> runAgain()
  {
    return new ReturnOrAwait<>(null, IntSets.emptySet(), null, true);
  }

  /**
   * Wait for all provided channels to become readable (or finished).
   *
   * Numbers in this set correspond to positions in the {@link FrameProcessor#inputChannels()} list.
   *
   * It is OK to pass in a mutable set, because this method does not modify the set or retain a reference to it.
   */
  public static <T> ReturnOrAwait<T> awaitAll(final IntSet await)
  {
    return new ReturnOrAwait<>(null, await, null, true);
  }

  /**
   * Wait for all of a certain number of channels.
   */
  public static <T> ReturnOrAwait<T> awaitAll(final int count)
  {
    return new ReturnOrAwait<>(null, rangeSet(count), null, true);
  }

  /**
   * Wait for all of the provided futures.
   */
  public static <T> ReturnOrAwait<T> awaitAllFutures(final Collection<ListenableFuture<?>> futures)
  {
    return new ReturnOrAwait<>(null, null, futures, true);
  }

  /**
   * Wait for any of the provided channels to become readable (or finished). When using this, callers should consider
   * removing any fully-processed and finished channels from the await-set, because if any channels in the await-set
   * are finished, the processor will run again immediately.
   *
   * Numbers in this set correspond to positions in the {@link FrameProcessor#inputChannels()} list.
   *
   * It is OK to pass in a mutable set, because this method does not modify the set or retain a reference to it.
   */
  public static <T> ReturnOrAwait<T> awaitAny(final IntSet await)
  {
    return new ReturnOrAwait<>(null, await, null, false);
  }

  /**
   * Return a result.
   */
  public static <T> ReturnOrAwait<T> returnObject(final T o)
  {
    return new ReturnOrAwait<>(o, null, null, false);
  }

  /**
   * The returned result. Valid if {@link #isReturn()} is true.
   */
  @Nullable
  public T value()
  {
    if (isAwait()) {
      throw new ISE("No value yet");
    }

    return retVal;
  }

  /**
   * The set of channels the processors wants to wait for. Valid if {@link #isAwait()} is true.
   *
   * Numbers in this set correspond to positions in the {@link FrameProcessor#inputChannels()} list.
   */
  public IntSet awaitableChannels()
  {
    if (!hasAwaitableChannels()) {
      throw new ISE("No await set");
    }

    return awaitChannels;
  }


  public Collection<ListenableFuture<?>> awaitableFutures()
  {
    if (!hasAwaitableFutures()) {
      throw new ISE("No futures set");
    }

    return awaitFutures;
  }

  /**
   * Whether the processor has returned a value.
   *
   * This is the opposite of {@link #isAwait()}.
   */
  public boolean isReturn()
  {
    return awaitChannels == null && awaitFutures == null;
  }

  /**
   * Whether the processor wants to be scheduled again.
   *
   * This is the opposite of {@link #isReturn()}.
   */
  public boolean isAwait()
  {
    return !isReturn();
  }

  /**
   * Whether the processor wants to wait for a set of futures. If true, {@link #awaitableFutures()} contains the
   * set of futures to wait for.
   */
  public boolean hasAwaitableFutures()
  {
    return awaitFutures != null;
  }

  /**
   * Whether the processor wants to wait for a set of channels. If true, {@link #awaitableChannels()} contains the
   * set of channels to wait for, and {@link #isAwaitAllChannels()}.
   */
  public boolean hasAwaitableChannels()
  {
    return awaitChannels != null;
  }

  /**
   * Whether the processor wants to wait for all channels in {@link #awaitableChannels()} (true), or any channel (false)
   */
  public boolean isAwaitAllChannels()
  {
    if (!hasAwaitableChannels()) {
      throw new ISE("No channels set");
    }

    return awaitAllChannels;
  }

  @Override
  public String toString()
  {
    if (hasAwaitableChannels()) {
      return "await channels=" + (awaitAllChannels ? "all" : "any") + awaitChannels;
    } else if (hasAwaitableFutures()) {
      return "await futures=" + awaitFutures;
    } else {
      return "return=" + retVal;
    }
  }

  private static IntSet rangeSet(final int count)
  {
    if (count == 0) {
      return IntSets.emptySet();
    } else if (count == 1) {
      return RANGE_SET_ONE;
    } else {
      return IntSets.fromTo(0, count);
    }
  }
}
