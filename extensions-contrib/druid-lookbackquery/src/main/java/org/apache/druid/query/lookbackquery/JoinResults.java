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

import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Function;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Result;

/**
 * Joins the result of a Timeseries measurement query and Timeseries cohort query
 */
abstract class JoinResults<K, V> implements CohortMapMakerYieldingAccumulator.ResultMap<K, V>,
    Function<Result<LookbackResultValue>, Result<LookbackResultValue>>
{

  private final Map<K, V> cohortResultMap = new HashMap<>();
  private final Period lookbackOffset;
  private final Future<Sequence<V>> cohortResult;
  private static final Logger LOGGER = new Logger(JoinResults.class);

  private Yielder<V> cohortYielder;
  private DateTime mapGeneration = DateTimes.MIN; // new DateTime(0); // initial time is the start of history

  public JoinResults(Future<Sequence<V>> cohortResult, Period lookbackOffset)
  {
    this.cohortResult = cohortResult;
    this.lookbackOffset = lookbackOffset;
  }

  @Override
  public void updateMapGeneration(DateTime dateTime)
  {
    this.mapGeneration = dateTime;
  }

  @Override
  public void clear()
  {
    cohortResultMap.clear();
  }

  @Override
  public void put(K time, V val)
  {
    cohortResultMap.put(time, val);
  }

  @Override
  public abstract DateTime extractTimestamp(V val);

  @Override
  public abstract K extractKey(V val);

  public abstract K extractOffsetKey(Result<LookbackResultValue> val, Period offset);

  public abstract Map<String, Object> extractResult(V cVal);

  @Override
  public Result<LookbackResultValue> apply(Result<LookbackResultValue> input)
  {
    // lazily create the cohortYielder the first time we access it
    if (cohortYielder == null) {
      try {
        cohortYielder = cohortResult.get().<V>toYielder(
            null,
            new CohortMapMakerYieldingAccumulator<K, V>(this)
        );
      }
      catch (InterruptedException | ExecutionException e) {
        LOGGER.error("Unexpected error occured while yielding");
        throw new InternalError(e);
      }
    }

    DateTime measurementTime = input.getTimestamp();
    // toss data from the cohort that is too early. cohortYielder.next() will update the map and resultMap
    // by pulling new data from the sequence until a new date is seen
    while (mapGeneration.isBefore(measurementTime.plus(lookbackOffset)) && !cohortYielder.isDone()) {
      cohortYielder = cohortYielder.next(cohortYielder.get());
    }

    // map has been updated, but make sure the last value is included
    if (cohortYielder.isDone()) {
      V val = cohortYielder.get();
      // get() can return null if nothing was in the cohort sequence
      if (val != null) {
        cohortResultMap.put(extractKey(val), val);
      }
    }

    K cohortKey = extractOffsetKey(input, lookbackOffset);
    V cohortMatchingResult = cohortResultMap.get(cohortKey);

    //generate the resulting sequence
    input.getValue().addLookbackValuesForKey(lookbackOffset, extractResult(cohortMatchingResult));
    return input;
  }

}
