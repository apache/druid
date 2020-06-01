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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;

/**
 * Creates {@link TimeDimTuple}s with time stamp adjust according to a {@link Granularity}.
 */
public class TimeDimTupleFactory
{
  private final Granularity granularity;

  public TimeDimTupleFactory(Granularity granularity)
  {
    this.granularity = granularity;
  }

  public TimeDimTuple createWithBucketedTimestamp(DateTime timestamp, String dimensionValue)
  {
    return new TimeDimTuple(getBucketTimestamp(timestamp), dimensionValue);
  }

  private long getBucketTimestamp(DateTime dateTime)
  {
    return granularity.bucketStart(dateTime).getMillis();
  }
}

