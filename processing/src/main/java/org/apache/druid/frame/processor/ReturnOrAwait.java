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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Instances of this class are returned by {@link FrameProcessor#runIncrementally}, and are used by
 * {@link FrameProcessorExecutor} to manage execution.
 *
 * An instance can be a "return" with a result, which means that the {@link FrameProcessor} is done working.
 * In this case {@link #isReturn()} is true and {@link #value()} contains the result.
 *
 * An instance can also be an "await", which means that the {@link FrameProcessor} wants to be scheduled again
 * in the future. In this case {@link #isAwait()} is true, {@link #awaitSet()} contains the set of input channels to
 * wait for, and {@link #isAwaitAll()} is whether the processor wants to wait for all channels, or any channel.
 */
public class ReturnOrAwait<T>
{
  private static final IntSet RANGE_SET_ONE = IntSets.singleton(0);

  @Nullable
  private final T retVal;

  @Nullable
  private final IntSet await;

  private final boolean awaitAll;

  private ReturnOrAwait(@Nullable T retVal, @Nullable IntSet await, final boolean awaitAll)
  {
    this.retVal = retVal;
    this.await = await;
    this.awaitAll = awaitAll;

    if (retVal != null && await != null) {
      throw new IAE("Cannot have a value when await != null");
    }
  }

  /**
   * Wait for nothing; that is: run again as soon as possible.
   */
  public static <T> ReturnOrAwait<T> runAgain()
  {
    return new ReturnOrAwait<>(null, IntSets.emptySet(), true);
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
    return new ReturnOrAwait<>(null, await, true);
  }

  /**
   * Wait for all of a certain number of channels.
   */
  public static <T> ReturnOrAwait<T> awaitAll(final int count)
  {
    return new ReturnOrAwait<>(null, rangeSet(count), true);
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
    return new ReturnOrAwait<>(null, await, false);
  }

  /**
   * Return a result.
   */
  public static <T> ReturnOrAwait<T> returnObject(final T o)
  {
    return new ReturnOrAwait<>(o, null, false);
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
  public IntSet awaitSet()
  {
    if (isReturn()) {
      throw new ISE("No await set");
    }

    return await;
  }

  /**
   * Whether the processor has returned a value.
   *
   * This is the opposite of {@link #isAwait()}.
   */
  public boolean isReturn()
  {
    return await == null;
  }

  /**
   * Whether the processor wants to be scheduled again.
   *
   * This is the opposite of {@link #isReturn()}.
   */
  public boolean isAwait()
  {
    return await != null;
  }

  /**
   * Whether the processor wants to wait for all channels in {@link #awaitSet()} (true), or any channel (false)
   */
  public boolean isAwaitAll()
  {
    return awaitAll;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReturnOrAwait<?> that = (ReturnOrAwait<?>) o;
    return awaitAll == that.awaitAll && Objects.equals(retVal, that.retVal) && Objects.equals(await, that.await);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(retVal, await, awaitAll);
  }

  @Override
  public String toString()
  {
    if (isAwait()) {
      return "await=" + (awaitAll ? "all" : "any") + await;
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
      final IntSet retVal = new IntOpenHashSet();

      for (int i = 0; i < count; i++) {
        retVal.add(i);
      }

      return retVal;
    }
  }
}
