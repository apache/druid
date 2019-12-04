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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Comparator;
import java.util.List;

/**
 * Interface representing Averager in the movingAverage query.
 *
 * @param <R> Type returned by the underlying averager.
 * @param <F> Type of finalized value.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "constant", value = ConstantAveragerFactory.class),
    @JsonSubTypes.Type(name = "doubleMean", value = DoubleMeanAveragerFactory.class),
    @JsonSubTypes.Type(name = "doubleMeanNoNulls", value = DoubleMeanNoNullAveragerFactory.class),
    @JsonSubTypes.Type(name = "doubleSum", value = DoubleSumAveragerFactory.class),
    @JsonSubTypes.Type(name = "doubleMax", value = DoubleMaxAveragerFactory.class),
    @JsonSubTypes.Type(name = "doubleMin", value = DoubleMinAveragerFactory.class),
    @JsonSubTypes.Type(name = "longMean", value = LongMeanAveragerFactory.class),
    @JsonSubTypes.Type(name = "longMeanNoNulls", value = LongMeanNoNullAveragerFactory.class),
    @JsonSubTypes.Type(name = "longSum", value = LongSumAveragerFactory.class),
    @JsonSubTypes.Type(name = "longMax", value = LongMaxAveragerFactory.class),
    @JsonSubTypes.Type(name = "longMin", value = LongMinAveragerFactory.class)
})
public interface AveragerFactory<R, F>
{
  int DEFAULT_PERIOD = 1;

  /**
   * Gets the column name that will be populated by the Averager
   *
   * @return The column name
   */
  String getName();

  /**
   * Returns the window size over which the averaging calculations will be
   * performed. Size is computed in terms of buckets rather than absolute time.
   *
   * @return The window size
   */
  int getNumBuckets();

  /**
   * Returns the cycle size (number of periods to skip during averaging calculations).
   *
   * @return The cycle size
   */
  int getCycleSize();

  /**
   * Create an Averager for a specific dimension combination.
   *
   * @return The {@link Averager}
   */
  Averager<R> createAverager();

  /**
   * Gets the list of dependent fields that will be used by this Averager. Most
   * {@link Averager}s depend on only a single field from the underlying query, but
   * that is not required. This method allow the required fields to be communicated
   * back to the main query so that validation to enforce the fields presence can
   * be accomplished.
   *
   * @return A list of field names
   */
  List<String> getDependentFields();

  /**
   * Returns a {@link Comparator} that can be used to compare result values for
   * purposes of sorting the end result of the query.
   *
   * @return A {@link Comparator}
   */
  Comparator<R> getComparator();

  /**
   * Finalize result value.
   *
   * @param val the value to finalize.
   *
   * @return The finalized value.
   */
  F finalizeComputation(R val);
}
