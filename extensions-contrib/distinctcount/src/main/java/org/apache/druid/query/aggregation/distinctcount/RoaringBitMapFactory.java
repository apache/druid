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

package org.apache.druid.query.aggregation.distinctcount;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;

public class RoaringBitMapFactory implements BitMapFactory
{
  private static final BitmapFactory BITMAP_FACTORY = new RoaringBitmapFactory();

  public RoaringBitMapFactory()
  {
  }

  @Override
  public MutableBitmap makeEmptyMutableBitmap()
  {
    return BITMAP_FACTORY.makeEmptyMutableBitmap();
  }

  @Override
  public String toString()
  {
    return "RoaringBitMapFactory";
  }

  @Override
  public boolean equals(Object o)
  {
    return this == o || o instanceof RoaringBitMapFactory;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }
}
