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

import java.util.Comparator;

/**
 * Base averager factory that adds a default comparable method.
 *
 * @param <R> return type
 * @param <F> finalized type
 */
public abstract class ComparableAveragerFactory<R extends Comparable<R>, F> extends BaseAveragerFactory<R, F>
{
  /**
   * Constructor.
   *
   * @param name       Name of the Averager
   * @param numBuckets Number of buckets in the analysis window
   * @param fieldName  Field from incoming events to include in the analysis
   * @param cycleSize  Cycle group size. Used to calculate day-of-week option. Default=1 (single element in group).
   */
  public ComparableAveragerFactory(String name, int numBuckets, String fieldName, Integer cycleSize)
  {
    super(name, numBuckets, fieldName, cycleSize);
  }

  @Override
  public Comparator<R> getComparator()
  {
    return Comparator.naturalOrder();
  }

}
