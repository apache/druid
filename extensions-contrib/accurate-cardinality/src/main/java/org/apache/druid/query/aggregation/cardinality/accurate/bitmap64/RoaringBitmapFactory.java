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

package org.apache.druid.query.aggregation.cardinality.accurate.bitmap64;


import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;

public class RoaringBitmapFactory implements BitmapFactory
{
  private static final ImmutableRoaringBitmap EMPTY_IMMUTABLE_BITMAP = new ImmutableRoaringBitmap();

  @Override
  public MutableBitmap makeEmptyMutableBitmap()
  {
    return new RoaringBitmap();
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return EMPTY_IMMUTABLE_BITMAP;
  }

  @Override
  public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    if (!(mutableBitmap instanceof RoaringBitmap)) {
      throw new IllegalStateException(StringUtils.format("Cannot convert [%s]", mutableBitmap.getClass()));
    }
    try {
      return ((RoaringBitmap) mutableBitmap).toImmutableBitmap();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public MutableBitmap mapMutableBitmap(ByteBuffer b)
  {
    return RoaringBitmap.deserializeFromByteArray(b.array());
  }
}
