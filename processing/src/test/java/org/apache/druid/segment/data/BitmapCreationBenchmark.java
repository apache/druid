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

package org.apache.druid.segment.data;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.logger.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 *
 */
@Disabled
@ParameterizedClass
@MethodSource("factoryClasses")
public class BitmapCreationBenchmark
{
  private static final Logger log = new Logger(BitmapCreationBenchmark.class);

  public static List<Class<? extends BitmapSerdeFactory>[]> factoryClasses()
  {
    return Arrays.asList(
        (Class<? extends BitmapSerdeFactory>[]) Collections.<Class<? extends BitmapSerdeFactory>>singletonList(
            ConciseBitmapSerdeFactory.class
        ).toArray(),
        (Class<? extends BitmapSerdeFactory>[]) Collections.<Class<? extends BitmapSerdeFactory>>singletonList(
            RoaringBitmapSerdeFactory.class
        ).toArray()
    );
  }

  final BitmapFactory factory;

  public BitmapCreationBenchmark(Class<? extends BitmapSerdeFactory> clazz)
      throws IllegalAccessException, InstantiationException
  {
    BitmapSerdeFactory serdeFactory = clazz.newInstance();
    factory = serdeFactory.getBitmapFactory();
  }

  private static final int NUM_BITS = 100000;


  static Random random;
  static int[] randIndex = new int[NUM_BITS];

  @AfterAll
  public static void cleanupAfterClass()
  {
    List<Class<? extends BitmapSerdeFactory>[]> classes = factoryClasses();
    for (int i = 0; i < classes.size(); ++i) {
      log.info("Entry [%d] is %s", i, classes.get(i)[0].getName());
    }
  }

  @BeforeAll
  public static void setupBeforeClass()
  {
    for (int i = 0; i < NUM_BITS; ++i) {
      randIndex[i] = i;
    }
    // Random seed chosen by hitting keyboard with BOTH hands... multiple times!
    random = new Random(78591378);
    for (int i = 0; i < NUM_BITS; ++i) {
      int idex = random.nextInt(randIndex.length);
      int swap = randIndex[i];
      randIndex[i] = randIndex[idex];
      randIndex[idex] = swap;
    }
  }

  ImmutableBitmap baseImmutableBitmap;
  MutableBitmap baseMutableBitmap;
  byte[] baseBytes;
  ByteBuffer baseByteBuffer;

  @BeforeEach
  public void setup()
  {
    baseMutableBitmap = factory.makeEmptyMutableBitmap();
    for (int i = 0; i < NUM_BITS; ++i) {
      baseMutableBitmap.add(i);
    }
    baseImmutableBitmap = factory.makeImmutableBitmap(baseMutableBitmap);
    baseBytes = baseImmutableBitmap.toBytes();
    baseByteBuffer = ByteBuffer.wrap(baseBytes);
  }
  @Test
  public void testLinearAddition()
  {
    MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    for (int i = 0; i < NUM_BITS; ++i) {
      mutableBitmap.add(i);
    }
    Assertions.assertEquals(NUM_BITS, mutableBitmap.size());
  }
  @Test
  public void testRandomAddition()
  {
    MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    for (int i : randIndex) {
      mutableBitmap.add(i);
    }
    Assertions.assertEquals(NUM_BITS, mutableBitmap.size());
  }
  @Test
  public void testLinearAdditionDescending()
  {
    MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    for (int i = NUM_BITS - 1; i >= 0; --i) {
      mutableBitmap.add(i);
    }
    Assertions.assertEquals(NUM_BITS, mutableBitmap.size());
  }
  @Test
  public void testToImmutableByteArray()
  {
    ImmutableBitmap immutableBitmap = factory.makeImmutableBitmap(baseMutableBitmap);
    Assertions.assertArrayEquals(baseBytes, immutableBitmap.toBytes());
  }
  @Test
  public void testFromImmutableByteArray()
  {
    ImmutableBitmap immutableBitmap = factory.mapImmutableBitmap(baseByteBuffer);
    Assertions.assertEquals(NUM_BITS, immutableBitmap.size());
  }

}
