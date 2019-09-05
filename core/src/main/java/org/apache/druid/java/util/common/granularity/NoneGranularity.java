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

package org.apache.druid.java.util.common.granularity;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;

/**
 * NoneGranularity does not bucket data
 */
public class NoneGranularity extends Granularity
{
  /**
   * This constructor is public b/c it is serialized and deserialized
   * based on type in GranularityModule
   */
  public NoneGranularity()
  {
  }

  @Override
  public DateTimeFormatter getFormatter(Formatter type)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  @Override
  public DateTime increment(DateTime time)
  {
    return time.plus(1);
  }

  @Override
  public DateTime bucketStart(DateTime time)
  {
    return time;
  }

  @Override
  public DateTime toDate(String filePath, Formatter formatter)
  {
    throw new UnsupportedOperationException("This method should not be invoked for this granularity type");
  }

  /**
   * Any interval is aligned with none granularity since it's effectively millisecond granularity.
   */
  @Override
  public boolean isAligned(Interval interval)
  {
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[]{0x0};
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return -1;
  }

  @Override
  public String toString()
  {
    return "NoneGranularity";
  }
}
