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

package org.apache.druid.queryng.operators;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Iterator over a sequence.
 */
public class SequenceIterator<T> implements Iterator<T>, Closeable
{
  private Yielder<T> yielder;

  public static <T> SequenceIterator<T> of(Sequence<T> sequence)
  {
    return new SequenceIterator<T>(sequence);
  }

  public SequenceIterator(Sequence<T> sequence)
  {
    this.yielder = sequence.toYielder(
        null,
        new YieldingAccumulator<T, T>()
        {
          @Override
          public T accumulate(T accumulated, T in)
          {
            yield();
            return in;
          }
        }
    );
  }

  @Override
  public boolean hasNext()
  {
    return !yielder.isDone();
  }

  @Override
  public T next()
  {
    Preconditions.checkState(!yielder.isDone());
    T value = yielder.get();
    yielder = yielder.next(null);
    return value;
  }

  @Override
  public void close()
  {
    if (yielder != null) {
      try {
        yielder.close();
      }
      catch (IOException e) {
        throw new RE(e, "Yielder failed to close");
      }
      yielder = null;
    }
  }
}
