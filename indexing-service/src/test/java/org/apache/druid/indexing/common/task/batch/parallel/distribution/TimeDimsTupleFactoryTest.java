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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TimeDimsTupleFactoryTest
{
  private static final Granularity GRANULARITY = Granularities.SECOND;
  private static final DateTime TIMESTAMP = DateTimes.utc(0);
  private static final List<Object> DIMENSIONS = ImmutableList.of("a", ImmutableList.of("m", "z"));

  private TimeDimsTupleFactory target;

  @Before
  public void setup()
  {
    target = new TimeDimsTupleFactory(GRANULARITY);
  }

  @Test
  public void adjustsTimestamps()
  {
    TimeDimsTuple timeDimsTuple = target.createWithBucketedTimestamp(TIMESTAMP, DIMENSIONS);
    Assert.assertEquals(TIMESTAMP.getMillis(), timeDimsTuple.getTimestamp());

    TimeDimsTuple timeDimsTuple_plus_1Msec = target.createWithBucketedTimestamp(TIMESTAMP.plus(1), DIMENSIONS);
    Assert.assertEquals(TIMESTAMP.getMillis(), timeDimsTuple_plus_1Msec.getTimestamp());

    TimeDimsTuple timeDimsTuple_plus_999Msec = target.createWithBucketedTimestamp(TIMESTAMP.plus(999), DIMENSIONS);
    Assert.assertEquals(TIMESTAMP.getMillis(), timeDimsTuple_plus_999Msec.getTimestamp());

    TimeDimsTuple timeDimsTuple_plus_1Sec = target.createWithBucketedTimestamp(TIMESTAMP.plus(1000), DIMENSIONS);
    Assert.assertEquals(TIMESTAMP.getMillis() + 1000, timeDimsTuple_plus_1Sec.getTimestamp());
  }

  @Test
  public void setsDimensionValues()
  {
    TimeDimsTuple timeDimsTuple = target.createWithBucketedTimestamp(TIMESTAMP, DIMENSIONS);
    Assert.assertEquals(DIMENSIONS, timeDimsTuple.getDimensionValues());
  }
}
