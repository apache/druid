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

package io.druid.java.util.common.guava;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

/**
 */
public class Sequences
{

  private static final EmptySequence EMPTY_SEQUENCE = new EmptySequence();

  public static <T> Sequence<T> simple(final Iterable<T> iterable)
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return iterable.iterator();
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {

          }
        }
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> Sequence<T> empty()
  {
    return (Sequence<T>) EMPTY_SEQUENCE;
  }

  public static <T> Sequence<T> concat(Sequence<T>... sequences)
  {
    return concat(Arrays.asList(sequences));
  }

  public static <T> Sequence<T> concat(Iterable<Sequence<T>> sequences)
  {
    return concat(Sequences.simple(sequences));
  }

  public static <T> Sequence<T> concat(Sequence<? extends Sequence<T>> sequences)
  {
    return new ConcatSequence<>(sequences);
  }

  public static <From, To> Sequence<To> map(Sequence<From> sequence, Function<? super From, ? extends To> fn)
  {
    return new MappedSequence<>(sequence, fn::apply);
  }

  public static <T> Sequence<T> filter(Sequence<T> sequence, Predicate<T> pred)
  {
    return new FilteredSequence<>(sequence, pred);
  }

  public static <T> Sequence<T> withBaggage(final Sequence<T> seq, final Closeable baggage)
  {
    Preconditions.checkNotNull(baggage, "baggage");
    return wrap(seq, new SequenceWrapper()
    {
      @Override
      public void after(boolean isDone, Throwable thrown) throws Exception
      {
        baggage.close();
      }
    });
  }

  /**
   * Allows to execute something before, after or around the processing of the given sequence. See documentation to
   * {@link SequenceWrapper} methods for some details.
   */
  public static <T> Sequence<T> wrap(Sequence<T> seq, SequenceWrapper wrapper)
  {
    Preconditions.checkNotNull(seq, "seq");
    Preconditions.checkNotNull(wrapper, "wrapper");
    return new WrappingSequence<>(seq, wrapper);
  }

  public static <T> Sequence<T> withEffect(final Sequence<T> seq, final Runnable effect, final Executor exec)
  {
    // Uses YieldingSequenceBase to be able to execute the effect if all elements of the wrapped seq are processed
    // (i. e. it "is done"), but the yielder of the underlying seq throws some exception from close(). This logic could
    // be found in ExecuteWhenDoneYielder.close(). If accumulate() is implemented manually in this anonymous class
    // instead of extending YieldingSequenceBase, it's not possible to distinguish exception thrown during elements
    // processing in accumulate() of the underlying seq, from exception thrown after all elements are processed,
    // in close().
    return new YieldingSequenceBase<T>()
    {
      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        return new ExecuteWhenDoneYielder<>(seq.toYielder(initValue, accumulator), effect, exec);
      }
    };
  }

  // This will materialize the entire sequence in memory. Use at your own risk.
  public static <T> Sequence<T> sort(final Sequence<T> sequence, final Comparator<T> comparator)
  {
    List<T> seqList = sequence.toList();
    Collections.sort(seqList, comparator);
    return simple(seqList);
  }

  private static class EmptySequence implements Sequence<Object>
  {
    @Override
    public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, Object> accumulator)
    {
      return initValue;
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, Object> accumulator)
    {
      return Yielders.done(initValue, null);
    }
  }
}
