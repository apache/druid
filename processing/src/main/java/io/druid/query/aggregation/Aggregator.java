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

package io.druid.query.aggregation;

/**
 * An Aggregator is an object that can aggregate metrics.  Its aggregation-related methods (namely, aggregate() and get())
 * do not take any arguments as the assumption is that the Aggregator was given something in its constructor that
 * it can use to get at the next bit of data.
 *
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate().  This is currently (as of this documentation) implemented through the use of Offset and
 * FloatColumnSelector objects.  The Aggregator has a handle on a FloatColumnSelector object which has a handle on an Offset.
 * QueryableIndex has both the Aggregators and the Offset object and iterates through the Offset calling the aggregate()
 * method on the Aggregators for each applicable row.
 *
 * This interface is old and going away.  It is being replaced by BufferAggregator
 */
public interface Aggregator {
  void aggregate();
  void reset();
  Object get();
  float getFloat();
  String getName();
  void close();
}
