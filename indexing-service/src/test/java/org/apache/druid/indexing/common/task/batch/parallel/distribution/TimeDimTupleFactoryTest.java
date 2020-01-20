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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TimeDimTupleFactoryTest
{
  private static final Granularity GRANULARITY = Granularities.SECOND;
  private static final DateTime TIMESTAMP = DateTimes.utc(0);
  private static final String DIMENSION_VALUE = "abc";

  private TimeDimTupleFactory target;

  @Before
  public void setup()
  {
    target = new TimeDimTupleFactory(GRANULARITY);
  }

  @Test
  public void adjustsTimestamps()
  {
    TimeDimTuple timeDimTuple = target.createWithBucketedTimestamp(TIMESTAMP, DIMENSION_VALUE);
    Assert.assertEquals(TIMESTAMP.getMillis(), timeDimTuple.getTimestamp());

    TimeDimTuple timeDimTuple_plus_1msec = target.createWithBucketedTimestamp(TIMESTAMP.plus(1), DIMENSION_VALUE);
    Assert.assertEquals(TIMESTAMP.getMillis(), timeDimTuple_plus_1msec.getTimestamp());

    TimeDimTuple timeDimTuple_plus_999msec = target.createWithBucketedTimestamp(TIMESTAMP.plus(999), DIMENSION_VALUE);
    Assert.assertEquals(TIMESTAMP.getMillis(), timeDimTuple_plus_999msec.getTimestamp());

    TimeDimTuple timeDimTuple_plus_1sec = target.createWithBucketedTimestamp(TIMESTAMP.plus(1000), DIMENSION_VALUE);
    Assert.assertEquals(TIMESTAMP.getMillis() + 1000, timeDimTuple_plus_1sec.getTimestamp());
  }

  @Test
  public void setsDimensionValue()
  {
    TimeDimTuple timeDimTuple = target.createWithBucketedTimestamp(TIMESTAMP, DIMENSION_VALUE);
    Assert.assertEquals(DIMENSION_VALUE, timeDimTuple.getDimensionValue());
  }
}
