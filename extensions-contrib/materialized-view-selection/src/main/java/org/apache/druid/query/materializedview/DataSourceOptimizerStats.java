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

package org.apache.druid.query.materializedview;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class DataSourceOptimizerStats 
{
  private final String base;
  private final long hitcount;
  private final long totalcount;
  private final long optimizerCost;
  private final Map<Set<String>, AtomicLong> missFields;
  private final Map<String, Long> derivativesHitCount;
  
  public DataSourceOptimizerStats(
      String base,
      long hitcount,
      long totalcount,
      long optimizerCost,
      Map<Set<String>, AtomicLong> missFields,
      Map<String, Long> derivativesHitCount
  )
  {
    this.base = base;
    this.hitcount = hitcount;
    this.totalcount = totalcount;
    this.optimizerCost = optimizerCost;
    this.missFields = missFields;
    this.derivativesHitCount = derivativesHitCount;
  }

  public Map<Set<String>, AtomicLong> getMissFields()
  {
    return missFields;
  }

  public String getBase()
  {
    return base;
  }

  public long getHitcount()
  {
    return hitcount;
  }

  public long getTotalcount()
  {
    return totalcount;
  }

  public double getOptimizerCost()
  {
    if (totalcount == 0L) {
      return 0;
    }
    return ((double) optimizerCost) / totalcount;
  }

  public double getHitRate()
  {
    if (totalcount == 0L) {
      return 0;
    }
    return ((double) hitcount) / totalcount;
  }

  public Map<String, Long> getDerivativesHitCount()
  {
    return derivativesHitCount;
  }
  
}
