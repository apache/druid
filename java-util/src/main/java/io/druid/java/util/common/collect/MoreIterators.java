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

package io.druid.java.util.common.collect;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class MoreIterators
{
  public static <X> Iterator<X> bracket(final Iterator<X> iterator, final Runnable before, final Runnable after)
  {
    return before(after(iterator, after), before);
  }

  /**
   * Run f immediately before the first element of iterator is generated.
   * Exceptions raised by f will prevent the requested behavior on the
   * underlying iterator, and can be handled by the caller.
   */
  public static <X> Iterator<X> before(final Iterator<X> iterator, final Runnable f)
  {
    return new Iterator<X>()
    {
      private final Runnable fOnlyOnce = new RunOnlyOnce(f);

      @Override
      public boolean hasNext()
      {
        fOnlyOnce.run();
        return iterator.hasNext();
      }

      @Override
      public X next()
      {
        fOnlyOnce.run();
        return iterator.next();
      }

      @Override
      public void remove()
      {
        fOnlyOnce.run();
        iterator.remove();
      }
    };
  }

  /**
   * Run f immediately after the last element of iterator is generated.
   * Exceptions must not be raised by f.
   */
  public static <X> Iterator<X> after(final Iterator<X> iterator, final Runnable f)
  {
    return new Iterator<X>()
    {
      private final Runnable fOnlyOnce = new RunOnlyOnce(f);

      @Override
      public boolean hasNext()
      {
        final boolean hasNext = iterator.hasNext();
        if (!hasNext) {
          fOnlyOnce.run();
        }
        return hasNext;
      }

      @Override
      public X next()
      {
        try {
          return iterator.next();
        }
        catch (NoSuchElementException e) {
          fOnlyOnce.run(); // (f exceptions are prohibited because they destroy e here)
          throw e;
        }
      }

      @Override
      public void remove()
      {
        iterator.remove();
      }
    };
  }

  private static class RunOnlyOnce implements Runnable
  {
    private final Runnable f;

    private volatile boolean hasRun = false;

    public RunOnlyOnce(Runnable f)
    {
      this.f = f;
    }

    @Override
    public void run()
    {
      if (!hasRun) {
        f.run();
        hasRun = true;
      }
    }
  }
}
