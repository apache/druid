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
  protected int cycleSize;

  /**
   * Constructor.
   *
   * @param name       Name of the Averager
   * @param numBuckets Number of buckets in the analysis window
   * @param fieldName  Field from incoming events to include in the analysis
   * @param cycleSize  Cycle group size. Used to calculate day-of-week option. Default=1 (single element in group).
   */
  public BaseAveragerFactory(String name, int numBuckets, String fieldName, Integer cycleSize)
  {
    this.name = name;
    this.numBuckets = numBuckets;
    this.fieldName = fieldName;
    this.cycleSize = (cycleSize != null) ? cycleSize : DEFAULT_PERIOD;
    Preconditions.checkNotNull(name, "Must have a valid, non-null averager name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null field name");
    Preconditions.checkArgument(this.cycleSize > 0, "Cycle size must be greater than zero");
    Preconditions.checkArgument(numBuckets > 0, "Bucket size must be greater than zero");
    Preconditions.checkArgument(!(this.cycleSize > numBuckets), "Cycle size must be less than the bucket size");
    Preconditions.checkArgument(numBuckets % this.cycleSize == 0, "cycleSize must devide numBuckets without a remainder");
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
  @JsonProperty("cycleSize")
  public int getCycleSize()
  {
    return cycleSize;
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
