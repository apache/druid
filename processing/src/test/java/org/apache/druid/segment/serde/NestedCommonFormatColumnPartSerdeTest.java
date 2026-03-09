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


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

class NestedCommonFormatColumnPartSerdeTest
{
  @Test
  void testSerde() throws JsonProcessingException
  {
    NestedCommonFormatColumnPartSerde partSerde =
        NestedCommonFormatColumnPartSerde.serializerBuilder()
                                         .withLogicalType(ColumnType.NESTED_DATA)
                                         .withHasNulls(true)
                                         .withEnforceLogicalType(false)
                                         .withColumnFormatSpec(
                                             NestedCommonFormatColumnFormatSpec.builder().build()
                                         )
                                         .withByteOrder(ByteOrder.nativeOrder())
                                         .build();
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    Assertions.assertEquals(partSerde, mapper.readValue(mapper.writeValueAsString(partSerde), ColumnPartSerde.class));
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(NestedCommonFormatColumnPartSerde.class)
                  .withIgnoredFields("serializer")
                  .usingGetClass()
                  .verify();
  }
}
