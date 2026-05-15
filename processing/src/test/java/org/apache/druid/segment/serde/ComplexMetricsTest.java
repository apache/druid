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

import org.apache.druid.query.aggregation.SerializablePairLongStringComplexMetricSerde;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ComplexMetricsTest
{

  @Test
  public void testRegister()
  {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(HyperUniquesSerde.TYPE_NAME);
    Assertions.assertNotNull(serde);
    Assertions.assertTrue(serde instanceof HyperUniquesSerde);
  }

  @Test
  public void testRegisterDuplicate()
  {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(HyperUniquesSerde.TYPE_NAME);
    Assertions.assertNotNull(serde);
    Assertions.assertTrue(serde instanceof HyperUniquesSerde);

    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    serde = ComplexMetrics.getSerdeForType(HyperUniquesSerde.TYPE_NAME);
    Assertions.assertNotNull(serde);
    Assertions.assertTrue(serde instanceof HyperUniquesSerde);
  }

  @Test
  public void testConflicting()
  {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(HyperUniquesSerde.TYPE_NAME);
    Assertions.assertNotNull(serde);
    Assertions.assertTrue(serde instanceof HyperUniquesSerde);

    IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new SerializablePairLongStringComplexMetricSerde())
    );
    Assertions.assertTrue(e.getMessage().contains("Incompatible serializer for type[hyperUnique] already exists."));
  }
}
