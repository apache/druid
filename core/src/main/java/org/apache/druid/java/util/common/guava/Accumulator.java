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

package org.apache.druid.java.util.common.guava;

/**
 * Stateless aggregator. Given a previous value and the current input value,
 * produce a new output value. The previous value is either an initial value
 * (for the first value in a group), or the previous output of the accumulator.
 * <p>
 * Used in multiple ways. Can create a collection of values if the accumulated
 * value is a collection, and the implementation adds to that collection. Can
 * aggregate values if the accumulated value is a total and the function adds
 * a new value to that total (where "total" and "add" can be any aggregation.)
 */
public interface Accumulator<AccumulatedType, InType>
{
  AccumulatedType accumulate(AccumulatedType accumulated, InType in);
}
