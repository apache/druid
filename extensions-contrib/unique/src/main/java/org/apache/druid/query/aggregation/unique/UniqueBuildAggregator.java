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

package org.apache.druid.query.aggregation.unique;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableArrayContainer;
import org.roaringbitmap.buffer.MappeableBitmapContainer;
import org.roaringbitmap.buffer.MappeableContainer;
import org.roaringbitmap.buffer.MappeableContainerPointer;
import org.roaringbitmap.buffer.MappeableRunContainer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.ArrayList;
import java.util.List;

public class UniqueBuildAggregator implements Aggregator
{
  private final ColumnValueSelector<Object> selector;
  private List<ImmutableRoaringBitmap> bitmaps;
  private final boolean useSortOr;

  public UniqueBuildAggregator(ColumnValueSelector<Object> selector, boolean useSortOr)
  {
    this.selector = selector;
    this.bitmaps = new ArrayList<>();
    this.useSortOr = useSortOr;
  }


  @Override
  public void aggregate()
  {
    final Object value = selector.getObject();
    if (value == null) {
      return;
    }
    if (value instanceof ImmutableRoaringBitmap) {
      bitmaps.add((ImmutableRoaringBitmap) value);
    } else {
      throw new UOE("Not implemented");
    }
  }

  @Override
  public ImmutableRoaringBitmap get()
  {
    if (bitmaps.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    if (useSortOr) {
      return batchOr(bitmaps);
    } else {
      return ImmutableRoaringBitmap.or(bitmaps.toArray(new ImmutableRoaringBitmap[0]));
    }
  }

  @Override
  public float getFloat()
  {
    throw new UOE("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UOE("Not implemented");
  }

  @Override
  public void close()
  {
    bitmaps = null;
  }

  public static ImmutableRoaringBitmap batchOr(List<ImmutableRoaringBitmap> bitmaps)
  {
    Object[] arr = new Object[65536];
    for (ImmutableRoaringBitmap bitmap : bitmaps) {
      MappeableContainerPointer p = bitmap.getContainerPointer();
      while (p.hasContainer()) {
        MappeableContainer c = p.getContainer();
        short k = p.key();
        MappeableContainer old = (MappeableContainer) arr[k];
        if (old == null) {
          arr[k] = c.clone().toBitmapContainer();
        } else if (old instanceof MappeableBitmapContainer) {
          ior(old, c);
        } else {
          arr[k] = or(old, c);
        }
        p.advance();
      }
    }
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    final int length = arr.length;
    for (int i = 0; i < length; i++) {
      MappeableContainer mc = (MappeableContainer) arr[i];
      if (mc != null) {
        bitmap.append((short) i, mc);
      }
    }
    return bitmap;
  }

  static MappeableContainer ior(MappeableContainer m1, MappeableContainer m2)
  {
    if (m2 instanceof MappeableArrayContainer) {
      return m1.ior((MappeableArrayContainer) m2);
    } else if (m2 instanceof MappeableRunContainer) {
      return m1.ior((MappeableRunContainer) m2);
    }

    return m1.ior((MappeableBitmapContainer) m2);
  }

  static MappeableContainer or(MappeableContainer m1, MappeableContainer m2)
  {
    if (m2 instanceof MappeableArrayContainer) {
      return m1.or((MappeableArrayContainer) m2);
    } else if (m2 instanceof MappeableRunContainer) {
      return m1.or((MappeableRunContainer) m2);
    }

    return m1.or((MappeableBitmapContainer) m2);
  }
}
