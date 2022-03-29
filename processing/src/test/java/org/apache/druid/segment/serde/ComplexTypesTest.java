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

import org.apache.druid.query.aggregation.SerializablePairLongStringSerde;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.junit.Assert;
import org.junit.Test;

public class ComplexTypesTest
{
  @Test
  public void testRegister()
  {
    ComplexTypes.registerSerde("hyperUnique", new HyperUniquesSerde());

    ComplexTypeSerde serde = ComplexTypes.getSerdeForType("hyperUnique");
    Assert.assertNotNull(serde);
    Assert.assertTrue(serde instanceof HyperUniquesSerde);
  }

  @Test
  public void testRegisterDuplicate()
  {
    ComplexTypes.registerSerde("hyperUnique", new HyperUniquesSerde());

    ComplexTypeSerde serde = ComplexTypes.getSerdeForType("hyperUnique");
    Assert.assertNotNull(serde);
    Assert.assertTrue(serde instanceof HyperUniquesSerde);

    ComplexTypes.registerSerde("hyperUnique", new HyperUniquesSerde());

    serde = ComplexTypes.getSerdeForType("hyperUnique");
    Assert.assertNotNull(serde);
    Assert.assertTrue(serde instanceof HyperUniquesSerde);
  }

  @Test
  public void testConflicting()
  {
    ComplexTypes.registerSerde("hyperUnique", new HyperUniquesSerde());

    ComplexTypeSerde serde = ComplexTypes.getSerdeForType("hyperUnique");
    Assert.assertNotNull(serde);
    Assert.assertTrue(serde instanceof HyperUniquesSerde);

    Assert.assertThrows(
        "Incompatible serializer for type[hyperUnique] already exists. Expected [org.apache.druid.query.aggregation.SerializablePairLongStringSerde], found [org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde",
        IllegalStateException.class,
        () -> ComplexTypes.registerSerde("hyperUnique", new SerializablePairLongStringSerde())
    );

    serde = ComplexTypes.getSerdeForType("hyperUnique");
    Assert.assertNotNull(serde);
    Assert.assertTrue(serde instanceof HyperUniquesSerde);
  }
}
