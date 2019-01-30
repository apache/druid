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

package org.apache.druid.query.movingaverage.averagers;

import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.Map;

/**
 * Interface for an averager
 *
 * @param <R> The return type of the averager
 */
public interface Averager<R>
{
  /**
   * Add a row to the window being operated on
   *
   * @param e      The row to add
   * @param aggMap The Map of AggregatorFactory used to determine if the metric should to be finalized
   */
  void addElement(Map<String, Object> e, Map<String, AggregatorFactory> aggMap);

  /**
   * There is a missing row, so record a missing entry in the window
   */
  void skip();

  /**
   * Compute the resulting "average" over the collected window
   *
   * @return the "average" over the window of buckets
   */
  R getResult();

  /**
   * @return the name
   */
  String getName();
}
