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

package org.apache.druid.segment.serde;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteOrder;

public class ColumnPartSerdeEqualsTest
{
  @ParameterizedTest
  @ValueSource(classes = {
      FloatNumericColumnPartSerde.class,
      FloatNumericColumnPartSerdeV2.class,
      DoubleNumericColumnPartSerde.class,
      DoubleNumericColumnPartSerdeV2.class,
      LongNumericColumnPartSerde.class,
      LongNumericColumnPartSerdeV2.class,
      DictionaryEncodedColumnPartSerde.class
  })
  public void testEqualsAndHashCode(Class<?> clazz)
  {
    // both bitmap serde factories hash to 0, so the hash can't vary with that field
    EqualsVerifier.forClass(clazz)
                  .usingGetClass()
                  .suppress(Warning.STRICT_HASHCODE)
                  .withIgnoredFields("serializer")
                  .withPrefabValues(ByteOrder.class, ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
                  .withPrefabValues(
                      BitmapSerdeFactory.class,
                      RoaringBitmapSerdeFactory.getInstance(),
                      new ConciseBitmapSerdeFactory()
                  )
                  .verify();
  }

  @Test
  public void testNullEqualsAndHashCode()
  {
    // both bitmap serde factories hash to 0, so the hash can't vary with that field
    EqualsVerifier.forClass(NullColumnPartSerde.class)
                  .usingGetClass()
                  .suppress(Warning.STRICT_HASHCODE)
                  .withPrefabValues(
                      BitmapSerdeFactory.class,
                      RoaringBitmapSerdeFactory.getInstance(),
                      new ConciseBitmapSerdeFactory()
                  )
                  .verify();
  }

  @Test
  public void testComplexEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ComplexColumnPartSerde.class)
                  .usingGetClass()
                  .withIgnoredFields("serializer")
                  .verify();
    Assertions.assertEquals(
        ComplexColumnPartSerde.createDeserializer("hyperUnique"),
        ComplexColumnPartSerde.createDeserializer("hyperUnique")
    );
    Assertions.assertNotEquals(
        ComplexColumnPartSerde.createDeserializer("hyperUnique"),
        ComplexColumnPartSerde.createDeserializer("other")
    );
  }
}
