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

package org.apache.druid.query.aggregation.firstlast;

import org.apache.druid.java.util.common.DateTimes;

/**
 * Differentiating factor between the first and the last aggregator. Specifies the
 */
public interface SelectionPredicate
{
  /**
   * @return Time value to initialize the aggregation with
   */
  long initValue();

  /**
   * @param currentTime  Time of the current row
   * @param selectedTime Aggregated time value
   * @return true if the current row should be selected over the aggregated value, else false
   */
  boolean apply(long currentTime, long selectedTime);

  SelectionPredicate FIRST_PREDICATE = new SelectionPredicate()
  {
    @Override
    public long initValue()
    {
      return DateTimes.MAX.getMillis();
    }

    @Override
    public boolean apply(long currentTime, long selectedTime)
    {
      return currentTime < selectedTime;
    }
  };

  SelectionPredicate LAST_PREDICATE = new SelectionPredicate()
  {
    @Override
    public long initValue()
    {
      return DateTimes.MIN.getMillis();
    }

    @Override
    public boolean apply(long currentTime, long selectedTime)
    {
      return currentTime >= selectedTime;
    }
  };
}
