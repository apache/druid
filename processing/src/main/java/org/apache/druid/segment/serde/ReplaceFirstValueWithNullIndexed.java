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

package org.apache.druid.segment.serde;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * An Indexed that replaces the first value with a literal null.
 *
 * Provided to enable compatibility for segments written under {@link NullHandling#sqlCompatible()} mode but
 * read under {@link NullHandling#replaceWithDefault()} mode.
 *
 * Important note: {@link #isSorted()} returns the same value as the underlying delegate. In this case, this class
 * assumes that {@code null} is the lowest possible value in the universe: including anything in {@link #delegate} and
 * anything that might be passed to {@link #indexOf(Object)}. Callers must ensure that this precondition is met.
 *
 * @see NullHandling#mustReplaceFirstValueWithNullInDictionary(Indexed)
 */
public class ReplaceFirstValueWithNullIndexed<T> implements Indexed<T>
{
  private final Indexed<T> delegate;

  public ReplaceFirstValueWithNullIndexed(Indexed<T> delegate)
  {
    this.delegate = delegate;

    if (delegate.size() < 1) {
      throw new ISE("Size[%s] must be >= 1", delegate.size());
    }
  }

  @Override
  public int size()
  {
    return delegate.size();
  }

  @Nullable
  @Override
  public T get(int index)
  {
    if (index == 0) {
      return null;
    } else {
      return delegate.get(index);
    }
  }

  @Override
  public int indexOf(@Nullable T value)
  {
    if (value == null) {
      return 0;
    } else {
      final int result = delegate.indexOf(value);
      if (result == 0 || result == -1) {
        return -2;
      } else {
        return result;
      }
    }
  }

  @Override
  public boolean isSorted()
  {
    return delegate.isSorted();
  }

  @Override
  public Iterator<T> iterator()
  {
    final Iterator<T> it = delegate.iterator();

    // Skip first value.
    it.next();

    class ReplaceFirstValueWithNullIndexedIterator implements Iterator<T>
    {
      boolean returnedNull;

      @Override
      public boolean hasNext()
      {
        return !returnedNull || it.hasNext();
      }

      @Override
      public T next()
      {
        if (!returnedNull) {
          returnedNull = true;
          return null;
        } else {
          return it.next();
        }
      }
    }

    return new ReplaceFirstValueWithNullIndexedIterator();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    delegate.inspectRuntimeShape(inspector);
  }
}
