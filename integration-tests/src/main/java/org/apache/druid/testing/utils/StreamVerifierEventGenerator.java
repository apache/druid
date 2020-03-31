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

package org.apache.druid.testing.utils;

import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.UUID;

public class StreamVerifierEventGenerator extends SyntheticStreamGenerator
{
  public StreamVerifierEventGenerator(int eventsPerSeconds, long cyclePaddingMs)
  {
    super(eventsPerSeconds, cyclePaddingMs);
  }

  @Override
  Object getEvent(int i, DateTime timestamp)
  {
    return StreamVerifierSyntheticEvent.of(
        UUID.randomUUID().toString(),
        timestamp.getMillis(),
        DateTimes.nowUtc().getMillis(),
        i,
        i == getEventsPerSecond() ? getSumOfEventSequence(getEventsPerSecond()) : null,
        i == 1
    );
  }


  /**
   * Assumes the first number in the sequence is 1, incrementing by 1, until numEvents.
   */
  private long getSumOfEventSequence(int numEvents)
  {
    return (numEvents * (1 + numEvents)) / 2;
  }
}
