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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.IAE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IdMappingTest
{
  @Test
  public void testMappingKnownCardinality()
  {
    final int cardinality = 10;
    IdMapping.Builder builder = IdMapping.Builder.ofCardinality(cardinality);
    for (int i = 0; i < cardinality; i++) {
      builder.addMapping(i * 10);
    }
    IdMapping mapping = builder.build();
    for (int i = 0; i < cardinality; i++) {
      Assertions.assertEquals(i * 10, mapping.getReverseId(i));
      Assertions.assertEquals(i, mapping.getForwardedId(i * 10));
    }

    Assertions.assertEquals(-1, mapping.getForwardedId(1));
    Assertions.assertEquals(-1, mapping.getForwardedId(-1));
    Assertions.assertEquals(-1, mapping.getReverseId(-1));
    Assertions.assertEquals(-1, mapping.getReverseId(10));
  }

  @Test
  public void testMappingUnknownCardinality()
  {
    final int cardinality = 10;
    IdMapping.Builder builder = IdMapping.Builder.ofUnknownCardinality();
    for (int i = 0; i < cardinality; i++) {
      builder.addForwardMapping(i * 10);
    }
    IdMapping mapping = builder.build();
    for (int i = 0; i < cardinality; i++) {
      Assertions.assertEquals(i * 10, mapping.getReverseId(i));
      Assertions.assertEquals(i, mapping.getForwardedId(i * 10));
    }

    Assertions.assertEquals(-1, mapping.getForwardedId(1));
    Assertions.assertEquals(-1, mapping.getForwardedId(-1));
    Assertions.assertEquals(-1, mapping.getReverseId(-1));
    Assertions.assertEquals(-1, mapping.getReverseId(10));
  }

  @Test
  public void testMappingCardinalityUnknownKnown()
  {
    final int cardinality = 10;
    IdMapping.Builder builder = IdMapping.Builder.ofUnknownCardinality();
    final IAE e = Assertions.assertThrows(
        IAE.class,
        () -> {
          for (int i = 0; i < cardinality; i++) {
            builder.addMapping(i * 10);
          }
        }
    );
    Assertions.assertTrue(e.getMessage().contains("addForwardMapping instead"));
  }

  @Test
  public void testMappingCardinalityKnownUnknown()
  {
    final int cardinality = 10;
    IdMapping.Builder builder = IdMapping.Builder.ofCardinality(cardinality);
    final IAE e = Assertions.assertThrows(
        IAE.class,
        () -> {
          for (int i = 0; i < cardinality; i++) {
            builder.addForwardMapping(i * 10);
          }
        }
    );
    Assertions.assertTrue(e.getMessage().contains("addMapping instead"));
  }
}
