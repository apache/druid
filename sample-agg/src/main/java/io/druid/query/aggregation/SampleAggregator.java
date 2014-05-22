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

import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

/**
 * @author Hagen Rother, hagen@rother.cc
 */
public class SampleAggregator  implements Aggregator {
  private final String name;
  private final ObjectColumnSelector selector;

  private Object value = null;

  public SampleAggregator(String name, ObjectColumnSelector selector) {
    this.name = name;
    this.selector = selector;
  }

  @Override
  public void aggregate() {
    if (value == null) {
      value = selector.get();
    }
  }

  @Override
  public void reset() {
    value = null;
  }

  @Override
  public Object get() {
    return value;
  }

  @Override
  public float getFloat() {
    throw new UnsupportedOperationException("SampleAggregator does not support getFloat()");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void close() {
    // nop
  }

  static final Comparator COMPARATOR = new Comparator() {
    @Override
    public int compare(Object o1, Object o2) {
      return o1.equals(o2) ? 0 : 1;
    }
  };
}
