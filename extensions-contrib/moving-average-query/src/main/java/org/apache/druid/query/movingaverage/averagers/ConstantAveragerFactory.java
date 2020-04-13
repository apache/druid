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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Implementation of AveragerFacvtory created solely for incremental development
 */

public class ConstantAveragerFactory implements AveragerFactory<Float, Float>
{

  private String name;
  private int numBuckets;
  private float retval;

  @JsonCreator
  public ConstantAveragerFactory(
      @JsonProperty("name") String name,
      @JsonProperty("buckets") int numBuckets,
      @JsonProperty("retval") float retval
  )
  {
    this.name = name;
    this.numBuckets = numBuckets;
    this.retval = retval;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  @JsonProperty("buckets")
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty
  public float getRetval()
  {
    return retval;
  }

  @Override
  public Averager<Float> createAverager()
  {
    return new ConstantAverager(numBuckets, name, retval);
  }

  @Override
  public List<String> getDependentFields()
  {
    return Collections.emptyList();
  }

  @Override
  public Comparator<Float> getComparator()
  {
    return Comparator.naturalOrder();
  }

  @Override
  public int getCycleSize()
  {
    return 1;
  }

  @Override
  public Float finalizeComputation(Float val)
  {
    return val;
  }
}
