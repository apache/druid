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

package io.druid.query.select;

import com.google.common.annotations.VisibleForTesting;

/**
 * offset iterator for select query
 */
public abstract class PagingOffset
{
  protected final int startOffset;
  protected final int threshold;

  protected int counter;

  public PagingOffset(int startOffset, int threshold)
  {
    this.startOffset = startOffset;
    this.threshold = threshold;
  }

  public abstract boolean isDescending();

  public final int startOffset()
  {
    return startOffset;
  }

  public abstract int startDelta();

  public final int threshold()
  {
    return threshold;
  }

  public final boolean hasNext()
  {
    return counter < threshold;
  }

  public final void next()
  {
    counter++;
  }

  public abstract int current();

  private static class Ascending extends PagingOffset
  {
    public Ascending(int offset, int threshold)
    {
      super(offset, threshold);
    }

    @Override
    public final boolean isDescending()
    {
      return false;
    }

    @Override
    public final int startDelta()
    {
      return startOffset;
    }

    @Override
    public final int current()
    {
      return startOffset + counter;
    }
  }

  private static class Descending extends PagingOffset
  {
    public Descending(int offset, int threshold)
    {
      super(offset, threshold);
    }

    @Override
    public final boolean isDescending()
    {
      return true;
    }

    @Override
    public final int startDelta()
    {
      return -startOffset - 1;
    }

    @Override
    public final int current()
    {
      return startOffset - counter;
    }
  }

  public static PagingOffset of(int startOffset, int threshold)
  {
    return startOffset < 0 ? new Descending(startOffset, threshold) : new Ascending(startOffset, threshold);
  }

  @VisibleForTesting
  static int toOffset(int delta, boolean descending)
  {
    if (delta < 0) {
      throw new IllegalArgumentException("Delta should not be negative");
    }
    return descending ? -delta - 1 : delta;
  }
}
