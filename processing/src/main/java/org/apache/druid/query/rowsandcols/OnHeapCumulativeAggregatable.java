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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A semantic interface used to cumulatively aggregate a list of AggregatorFactories across a given set of data
 * <p>
 * The aggregation specifically happens on-heap and should be used in places where it is known that the data
 * set can be worked with entirely on-heap.
 * <p>
 * Note, as we implement frame-handling for window aggregations, it is expected that this interface will undergo a
 * transformation.  It might be deleted and replaced with something else, or might just see a change done in place.
 * Either way, there is no assumption of enforced compatibility with this interface at this point in time.
 */
public interface OnHeapCumulativeAggregatable
{
  /**
   * Cumulatively aggregates the data using the {@code List<AggregatorFactory} objects.
   *
   * @param aggFactories definition of aggregations to be done
   * @return a list of objects, one per AggregatorFactory.  That is, the length of the return list should be equal to
   * the length of the aggFactories list passed as an argument, while the length of the internal {@code Object[]} will
   * be equivalent to the number of rows
   */
  ArrayList<Object[]> aggregateCumulative(List<AggregatorFactory> aggFactories);
}
