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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.nio.ByteOrder;

public class NestedCommonFormatColumnPartSerdeTest
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde()
  {
    NestedCommonFormatColumnPartSerde partSerde = NestedCommonFormatColumnPartSerde.serializerBuilder()
                                                                                   .isConstant(false)
                                                                                   .isVariantType(false)
                                                                                   .withByteOrder(ByteOrder.nativeOrder())
                                                                                   .withHasNulls(true)
                                                                                   .withLogicalType(ColumnType.LONG)
                                                                                   .build();
  }
}
