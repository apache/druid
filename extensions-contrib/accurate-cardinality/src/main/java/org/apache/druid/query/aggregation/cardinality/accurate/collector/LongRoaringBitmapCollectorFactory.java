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

package org.apache.druid.query.aggregation.cardinality.accurate.collector;


import org.apache.druid.query.aggregation.cardinality.accurate.bitmap64.LongBitmapFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.bitmap64.LongRoaringBitmapFactory;

import java.nio.ByteBuffer;

public class LongRoaringBitmapCollectorFactory implements LongBitmapCollectorFactory
{
  private static final LongBitmapFactory LONG_BITMAP_FACTORY = new LongRoaringBitmapFactory();

  public LongRoaringBitmapCollectorFactory()
  {
  }

  @Override
  public LongBitmapCollector makeEmptyCollector()
  {
    return new LongRoaringBitmapCollector(LONG_BITMAP_FACTORY.makeEmptyMutableBitmap());
  }

  @Override
  public LongBitmapCollector makeCollector(ByteBuffer buffer)
  {
    return new LongRoaringBitmapCollector(LONG_BITMAP_FACTORY.mapMutableBitmap(buffer));
  }

  @Override
  public String toString()
  {
    return "LongRoaringBitmapCollectorFactory";
  }

  @Override
  public boolean equals(Object o)
  {
    return this == o || o instanceof LongRoaringBitmapCollectorFactory;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }
}
