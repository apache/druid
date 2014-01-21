/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation;

import com.google.common.hash.Hashing;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import gnu.trove.map.TIntByteMap;
import gnu.trove.map.hash.TIntByteHashMap;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class HyperloglogAggregator implements Aggregator
{
  private static final Logger log = new Logger(HyperloglogAggregator.class);

  public static final int log2m = 12;
  public static final int m = (int) Math.pow(2, log2m);
  public static final double alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m;

  private final String name;
  private final ObjectColumnSelector selector;

  private TIntByteHashMap ibMap;

  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return o.equals(o1) ? 0 : 1;
    }
  };

  public static Object combine(Object lhs, Object rhs)
  {
    final TIntByteMap newIbMap = new TIntByteHashMap((TIntByteMap) lhs);
    final TIntByteMap rightIbMap = (TIntByteMap) rhs;
    final int[] keys = rightIbMap.keys();

    for (int key : keys) {
      if (newIbMap.get(key) == newIbMap.getNoEntryValue() || rightIbMap.get(key) > newIbMap.get(key)) {
        newIbMap.put(key, rightIbMap.get(key));
      }
    }

    return newIbMap;
  }

  public HyperloglogAggregator(String name, ObjectColumnSelector selector)
  {
    this.name = name;
    this.selector = selector;
    this.ibMap = new TIntByteHashMap();
  }

  @Override
  public void aggregate()
  {
    final Object value = selector.get();

    if (value == null) {
      return;
    }

    if (value instanceof TIntByteHashMap) {
      final TIntByteHashMap newIbMap = (TIntByteHashMap) value;
      final int[] indexes = newIbMap.keys();

      for (int index : indexes) {
        if (ibMap.get(index) == ibMap.getNoEntryValue() || newIbMap.get(index) > ibMap.get(index)) {
          ibMap.put(index, newIbMap.get(index));
        }
      }
    } else if (value instanceof String) {
      log.debug("value [%s]", selector.get());

      final long id = Hashing.murmur3_128().hashString((String) (value)).asLong();
      final int bucket = (int) (id >>> (Long.SIZE - log2m));
      final int zerolength = Long.numberOfLeadingZeros((id << log2m) | (1 << (log2m - 1)) + 1) + 1;

      if (ibMap.get(bucket) == ibMap.getNoEntryValue() || ibMap.get(bucket) < (byte) zerolength) {
        ibMap.put(bucket, (byte) zerolength);
      }
    } else {
      throw new ISE("Aggregate does not support values of type[%s]", value.getClass().getName());
    }
  }

  @Override
  public void reset()
  {
    this.ibMap = new TIntByteHashMap();
  }

  @Override
  public Object get()
  {
    return ibMap;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("HyperloglogAggregator does not support getFloat()");
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
    // do nothing
  }
}
