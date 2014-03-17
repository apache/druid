/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.aggregation.hyperloglog;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public class HyperUniquesAggregator implements Aggregator
{
  private final String name;
  private final ObjectColumnSelector selector;

  private HyperLogLogCollector collector;

  public HyperUniquesAggregator(
      String name,
      ObjectColumnSelector selector
  )
  {
    this.name = name;
    this.selector = selector;

    this.collector = HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public void aggregate()
  {
    collector.fold((HyperLogLogCollector) selector.get());
  }

  @Override
  public void reset()
  {
    collector = HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public Object get()
  {
    return collector;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public Aggregator clone()
  {
    return new HyperUniquesAggregator(name, selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
