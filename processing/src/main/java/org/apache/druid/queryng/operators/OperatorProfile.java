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

package org.apache.druid.queryng.operators;

import java.util.HashMap;
import java.util.Map;

/**
 * Performance report for one operator.
 */
public class OperatorProfile
{
  public static final String CURSOR_COUNT_METRIC = "cursor-count";
  public static final String ROW_COUNT_METRIC = "row-count";
  public static final String BATCH_COUNT_METRIC = "batch-count";
  // To be used later.
  //public static final String ELAPSED_MS_METRIC = "elapsed-ms";
  // To be used later.
  //public static final String ACTIVE_MS_METRIC = "active-ms";
  public static final String CPU_TIME_NS = "cpu-time-ns";

  public static final String VECTORIZED_ATTRIB = "vectorized";

  public final String operatorName;
  public boolean omitFromProfile;
  private Map<String, Object> metrics = new HashMap<>();

  public OperatorProfile(String operatorName)
  {
    this.operatorName = operatorName;
    this.omitFromProfile = false;
  }

  public OperatorProfile(String operatorName, boolean omit)
  {
    this.operatorName = operatorName;
    this.omitFromProfile = omit;
  }

  public static OperatorProfile silentOperator(String name)
  {
    return new OperatorProfile(name, true);
  }

  public static OperatorProfile silentOperator(Operator<?> op)
  {
    return silentOperator(op.getClass().getSimpleName());
  }

  public void add(String key, Object value)
  {
    metrics.put(key, value);
  }

  public Map<String, Object> metrics()
  {
    return metrics;
  }
}
