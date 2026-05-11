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

package org.apache.druid.segment.column;

import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.serde.LongNumericColumnPartSerde;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.util.List;

class ColumnDescriptorTest
{
  @Test
  void testToColumnTypeFallsBackToValueTypeForSimpleSerdes()
  {
    // simple numeric serdes don't carry extra type info, so toColumnType() falls back to ValueType
    final ColumnPartSerde longSerde = LongNumericColumnPartSerde.serializerBuilder()
                                                                .withByteOrder(ByteOrder.nativeOrder())
                                                                .build();
    final ColumnDescriptor descriptor = new ColumnDescriptor(ValueType.LONG, false, List.of(longSerde));

    Assertions.assertEquals(ColumnType.LONG, descriptor.toColumnType());
  }

  @Test
  void testToColumnTypeUsesComplexTypeNameFromPartSerde()
  {
    // complex columns carry the complex type name in the part serde; toColumnType() must preserve it
    final ColumnPartSerde complexSerde = ComplexColumnPartSerde.serializerBuilder()
                                                               .withTypeName("hyperUnique")
                                                               .build();
    final ColumnDescriptor descriptor = new ColumnDescriptor(ValueType.COMPLEX, false, List.of(complexSerde));

    final ColumnType columnType = descriptor.toColumnType();
    Assertions.assertEquals(ValueType.COMPLEX, columnType.getType());
    Assertions.assertEquals("hyperUnique", columnType.getComplexTypeName());
  }

  @Test
  void testToColumnTypeUsesLogicalTypeFromNestedPartSerde()
  {
    // nested common format columns carry the full logical type (including array element types) in the part serde
    final ColumnPartSerde nestedSerde =
        NestedCommonFormatColumnPartSerde.serializerBuilder()
                                         .withLogicalType(ColumnType.STRING_ARRAY)
                                         .withHasNulls(false)
                                         .withColumnFormatSpec(NestedCommonFormatColumnFormatSpec.builder().build())
                                         .withByteOrder(ByteOrder.nativeOrder())
                                         .build();
    final ColumnDescriptor descriptor = new ColumnDescriptor(ValueType.ARRAY, false, List.of(nestedSerde));

    Assertions.assertEquals(ColumnType.STRING_ARRAY, descriptor.toColumnType());
  }

  @Test
  void testToColumnTypeEmptyPartsFallsBackToValueType()
  {
    final ColumnDescriptor descriptor = new ColumnDescriptor(ValueType.STRING, false, List.of());
    final ColumnType columnType = descriptor.toColumnType();
    Assertions.assertEquals(ValueType.STRING, columnType.getType());
    Assertions.assertNull(columnType.getComplexTypeName());
  }
}
