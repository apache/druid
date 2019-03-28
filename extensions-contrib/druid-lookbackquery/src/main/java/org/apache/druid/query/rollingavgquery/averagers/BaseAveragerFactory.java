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

package org.apache.druid.query.rollingavgquery.averagers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;

/**
 * Common base class for AveragerFactories
 *
 * @param <R> Base type that the averager should return as a result
 * @param <F> Type that that is returned from finalization
 */
public abstract class BaseAveragerFactory<R, F> implements AveragerFactory<R, F>
{

  protected String name;
  protected String fieldName;
  protected int numBuckets;
  protected int period;

  /**
   * Constructor.
   *
   * @param name       Name of the Averager
   * @param numBuckets Number of buckets in the analysis window
   * @param fieldName  Field from incoming events to include in the analysis
   * @param period     The number of periods to ignore within the bucket on which averaging calculations are performed
   */
  public BaseAveragerFactory(String name, int numBuckets, String fieldName, Integer period)
  {
    this.name = name;
    this.numBuckets = numBuckets;
    this.fieldName = fieldName;
    this.period = (period != null) ? period : DEFAULT_PERIOD;
    Preconditions.checkNotNull(name, "Must have a valid, non-null averager name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null field name");
    Preconditions.checkArgument(this.period > 0, "Period must be greater than zero");
    Preconditions.checkArgument(numBuckets > 0, "Bucket size must be greater than zero");
    Preconditions.checkArgument(this.period < numBuckets, "Period must be less than the bucket size");
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  @JsonProperty("buckets")
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @Override
  @JsonProperty("periods")
  public int getPeriod()
  {
    return period;
  }

  @Override
  public List<String> getDependentFields()
  {
    return Collections.singletonList(fieldName);
  }

  @SuppressWarnings("unchecked")
  @Override
  public F finalizeComputation(R val)
  {
    return (F) val;
  }
}
