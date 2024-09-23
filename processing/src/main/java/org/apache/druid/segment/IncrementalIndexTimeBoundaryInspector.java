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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;

/**
 * {@link TimeBoundaryInspector} for {@link IncrementalIndex}.
 */
public class IncrementalIndexTimeBoundaryInspector implements TimeBoundaryInspector
{
  private final IncrementalIndex index;

  public IncrementalIndexTimeBoundaryInspector(IncrementalIndex index)
  {
    this.index = index;
  }

  @Override
  public DateTime getMinTime()
  {
    final DateTime minTime = index.getMinTime();
    return minTime == null ? DateTimes.MIN : minTime;
  }

  @Override
  public DateTime getMaxTime()
  {
    final DateTime maxTime = index.getMaxTime();
    return maxTime == null ? DateTimes.MAX : maxTime;
  }

  @Override
  public boolean isMinMaxExact()
  {
    return !index.isEmpty();
  }
}
