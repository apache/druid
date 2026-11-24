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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.guava.Comparators;

import java.util.List;

/**
 * Creates a sorted dictionary given some existing dictionary, storing a mapping of both sorted id to unsorted id
 * and unsorted id to sorted id. Values are sorted by natural order with nulls first.
 */
public class SortedDimensionDictionary<T extends Comparable<T>> extends ComparatorSortedDimensionDictionary<T>
{
  /**
   * @param idToValue unsorted dictionary values, which must be unique. The sort looks up values by id many
   *                  times, so this should be a {@link java.util.RandomAccess} list such as
   *                  {@link java.util.ArrayList}.
   * @param length    number of ids, from the start of {@code idToValue}, to include
   */
  public SortedDimensionDictionary(List<T> idToValue, int length)
  {
    super(idToValue, Comparators.naturalNullsFirst(), length);
  }
}
