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

package org.apache.druid.collections.bitmap;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class WrappedRoaringBitmapTest
{
  private final RoaringBitmapFactory factory;

  public WrappedRoaringBitmapTest(RoaringBitmapFactory factory)
  {
    this.factory = factory;
  }

  @Parameterized.Parameters
  public static List<RoaringBitmapFactory[]> factoryClasses()
  {
    return Arrays.asList(
        new RoaringBitmapFactory[] {
            new RoaringBitmapFactory(false)
        },
        new RoaringBitmapFactory[] {
            new RoaringBitmapFactory(true)
        }
    );
  }

  private WrappedRoaringBitmap createWrappedRoaringBitmap()
  {
    WrappedRoaringBitmap set = (WrappedRoaringBitmap) factory.makeEmptyMutableBitmap();
    set.add(1);
    set.add(3);
    set.add(5);
    set.add(7);
    set.add(9);
    return set;
  }

  @Test
  public void testSerialize()
  {
    WrappedRoaringBitmap set = createWrappedRoaringBitmap();

    byte[] buffer = new byte[set.getSizeInBytes()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    set.serialize(byteBuffer);
    byteBuffer.flip();
    ImmutableBitmap immutableBitmap = new RoaringBitmapFactory().mapImmutableBitmap(byteBuffer);
    Assert.assertEquals(5, immutableBitmap.size());
  }

  @Test
  public void testToByteArray()
  {
    WrappedRoaringBitmap set = createWrappedRoaringBitmap();
    ImmutableBitmap immutableBitmap = new RoaringBitmapFactory().mapImmutableBitmap(ByteBuffer.wrap(set.toBytes()));
    Assert.assertEquals(5, immutableBitmap.size());
  }

}
