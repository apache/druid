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

package org.apache.druid.query.aggregation;

import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ObjectStrategy;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;

public class DateTimeSerde extends BasicComplexMetricSerde<DateTime>
{
  public static final ColumnType TYPE = ColumnType.ofComplex("dateTime");

  public DateTimeSerde()
  {
    super(TYPE, new DateTimeObjectStrategy());
  }

  static class DateTimeObjectStrategy implements ObjectStrategy<DateTime>
  {

    @Override
    public int compare(DateTime o1, DateTime o2)
    {
      return o1.compareTo(o2);
    }

    @Override
    public Class<? extends DateTime> getClazz()
    {
      return DateTime.class;
    }

    @Override
    public DateTime fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      throw DruidException.defensive("not supported");
    }

    @Override
    public byte[] toBytes(DateTime val)
    {
      throw DruidException.defensive("not supported");
    }
  }
}
