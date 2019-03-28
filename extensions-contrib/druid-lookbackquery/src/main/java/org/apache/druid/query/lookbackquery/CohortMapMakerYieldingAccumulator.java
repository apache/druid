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

package org.apache.druid.query.lookbackquery;

import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.joda.time.DateTime;

/**
 * A Yielder for incrementally consuming the cohort result set. Each call to next() consumes
 * all rows for the next time increment from the cohort query, inserts them into a map for use
 * in the join, and sets up for the next call.
 *
 * @param <T>
 */
class CohortMapMakerYieldingAccumulator<K, T> extends YieldingAccumulator<T, T>
{
  interface ResultMap<K, V>
  {
    void updateMapGeneration(DateTime dateTime);

    void clear();

    void put(K time, V val);

    DateTime extractTimestamp(V val);

    K extractKey(V val);
  }

  final CohortMapMakerYieldingAccumulator.ResultMap<K, T> resultMap;

  public CohortMapMakerYieldingAccumulator(CohortMapMakerYieldingAccumulator.ResultMap<K, T> resultMap)
  {
    this.resultMap = resultMap;
  }

  @Override
  public void reset()
  {
    super.reset();
    resultMap.clear();
  }

  @Override
  public T accumulate(T initval, T in)
  {
    DateTime initValTime = resultMap.extractTimestamp(initval);

    if (initval != null) {
      K key = resultMap.extractKey(initval);

      resultMap.put(key, initval);
      resultMap.updateMapGeneration(initValTime);
    }

    DateTime inTime = resultMap.extractTimestamp(in);
    if (initValTime != null && !inTime.equals(initValTime)) {
      yield();
    }
    return in;
  }

}
