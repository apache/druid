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

import java.util.List;

/**
 * Creates {@link TimeDimsTuple}s with time stamp adjust according to a {@link Granularity}.
 */
public class TimeDimsTupleFactory
{
  private final Granularity granularity;

  public TimeDimsTupleFactory(Granularity granularity)
  {
    this.granularity = granularity;
  }

  public TimeDimsTuple createWithBucketedTimestamp(DateTime timestamp, List<Object> dimensionValues)
  {
    return new TimeDimsTuple(getBucketTimestamp(timestamp), dimensionValues);
  }

  private long getBucketTimestamp(DateTime dateTime)
  {
    return granularity.bucketStart(dateTime).getMillis();
  }
}

