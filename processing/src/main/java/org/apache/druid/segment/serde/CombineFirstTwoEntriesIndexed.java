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

import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;

/**
 * An {@link Indexed} that delegates to an underyling instance, but combines the first two entries.
 *
 * Unlike {@link CombineFirstTwoValuesIndexedInts}, this class combines the first two *entries*.
 * So [0, 1, 2] becomes [(something), 2]. The first two entries, 0 and 1, were replaced with (something). That something
 * is given by {@link #newFirstValue()}.
 *
 * Provided to enable compatibility for segments written under {@link NullHandling#sqlCompatible()} mode but
 * read under {@link NullHandling#replaceWithDefault()} mode.
 *
 * Important note: {@link #isSorted()} returns the same value as the underlying delegate. In this case, this class
 * assumes that {@link #newFirstValue()} is the lowest possible value in the universe: including anything in
 * {@link #delegate} and anything that might be passed to {@link #indexOf(Object)}. Callers must ensure that this
 * precondition is met.
 *
 * @see NullHandling#mustCombineNullAndEmptyInDictionary(Indexed)
 */
public abstract class CombineFirstTwoEntriesIndexed<T> implements Indexed<T>
{
  private static final int FIRST_ID = 0;

  protected final Indexed<T> delegate;

  protected CombineFirstTwoEntriesIndexed(Indexed<T> delegate)
  {
    this.delegate = delegate;

    if (delegate.size() < 2) {
      throw new ISE("Size[%s] must be >= 2", delegate.size());
    }
  }

  /**
   * Combine the first two values into a literal null.
   */
  public static <T> CombineFirstTwoEntriesIndexed<T> returnNull(final Indexed<T> delegate)
  {
    return new CombineFirstTwoEntriesIndexed<>(delegate)
    {
      @Nullable
      @Override
      protected T newFirstValue()
      {
        return null;
      }
    };
  }

  /**
   * Union the first two bitmaps.
   */
  public static CombineFirstTwoEntriesIndexed<ImmutableBitmap> unionBitmaps(
      final BitmapFactory bitmapFactory,
      final Indexed<ImmutableBitmap> delegate
  )
  {
    return new CombineFirstTwoEntriesIndexed<>(delegate)
    {
      @Nullable
      @Override
      protected ImmutableBitmap newFirstValue()
      {
        return bitmapFactory.union(ImmutableList.of(delegate.get(FIRST_ID), delegate.get(FIRST_ID + 1)));
      }
    };
  }

  @Nullable
  protected abstract T newFirstValue();

  @Override
  public int size()
  {
    return delegate.size() - 1;
  }

  @Nullable
  @Override
  public T get(int index)
  {
    if (index == FIRST_ID) {
      return newFirstValue();
    } else {
      return delegate.get(index + 1);
    }
  }

  @Override
  public int indexOf(@Nullable T value)
  {
    if (Objects.equals(newFirstValue(), value)) {
      return FIRST_ID;
    } else {
      final int index = delegate.indexOf(value);

      if (index > FIRST_ID + 1) {
        // Item found, index needs adjustment.
        return index - 1;
      } else if (index >= 0) {
        // Item found, but shadowed, so really not found.
        // Insertion point is after FIRST_ID. (See class-level javadoc: newFirstValue is required to be
        // lower than all elements in the universe.)
        return -2;
      } else if (index >= -2) {
        // Item not found, and insertion point is prior to, or within, the shadowed portion of delegate. Return
        // insertion point immediately after newFirstValue, since that value is required to be lower than all elements
        // in the universe.
        return -2;
      } else {
        // Item not found, and insertion point is after the shadowed portion of delegate. Adjust and return.
        return index + 1;
      }
    }
  }

  @Override
  public Iterator<T> iterator()
  {
    final Iterator<T> it = delegate.iterator();

    // Skip first two values.
    //CHECKSTYLE.OFF: Regexp
    it.next();
    it.next();
    //CHECKSTYLE.ON: Regexp

    class CoalescingIndexedIterator implements Iterator<T>
    {
      boolean returnedFirstValue;

      @Override
      public boolean hasNext()
      {
        return !returnedFirstValue || it.hasNext();
      }

      @Override
      public T next()
      {
        if (!returnedFirstValue) {
          returnedFirstValue = true;
          return newFirstValue();
        } else {
          return it.next();
        }
      }
    }

    return new CoalescingIndexedIterator();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    delegate.inspectRuntimeShape(inspector);
  }

  @Override
  public boolean isSorted()
  {
    return delegate.isSorted();
  }
}
