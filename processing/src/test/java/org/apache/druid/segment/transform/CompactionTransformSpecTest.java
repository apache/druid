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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class CompactionTransformSpecTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(CompactionTransformSpec.class)
                  .withNonnullFields("filter")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    final CompactionTransformSpec expected = new CompactionTransformSpec(
        new SelectorDimFilter("dim1", "foo", null),
        VirtualColumns.create(
            new ExpressionVirtualColumn(
                "isRobotFiltered",
                "concat(isRobot, '_filtered')",
                ColumnType.STRING,
                ExprMacroTable.nil()
            )
        )
    );
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ExprMacroTable.class, TestExprMacroTable.INSTANCE)
    );
    final byte[] json = mapper.writeValueAsBytes(expected);
    final CompactionTransformSpec fromJson = (CompactionTransformSpec) mapper.readValue(
        json,
        CompactionTransformSpec.class
    );
    Assertions.assertEquals(expected, fromJson);
  }

  @Test
  public void testSerde_emptyVirtualColumns_notSerializedForFingerprintConsistency() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();

    // Spec with VirtualColumns.EMPTY (new code path)
    final CompactionTransformSpec withEmpty = new CompactionTransformSpec(
        new SelectorDimFilter("dim1", "foo", null),
        VirtualColumns.EMPTY
    );

    // Spec with null (old code path before VirtualColumns support)
    final CompactionTransformSpec withNull = new CompactionTransformSpec(
        new SelectorDimFilter("dim1", "foo", null),
        null
    );

    // Serialize both
    final String jsonWithEmpty = mapper.writeValueAsString(withEmpty);
    final String jsonWithNull = mapper.writeValueAsString(withNull);

    // Both should produce identical JSON (no virtualColumns field)
    // This ensures fingerprint consistency: old segments (no VC) match new segments (EMPTY VC)
    Assertions.assertEquals(
        jsonWithNull,
        jsonWithEmpty,
        "VirtualColumns.EMPTY should serialize identically to null for fingerprint consistency"
    );

    // Verify virtualColumns field is not present in either JSON
    Assertions.assertFalse(
        jsonWithEmpty.contains("virtualColumns"),
        "virtualColumns field should not appear in JSON when empty"
    );
    Assertions.assertFalse(
        jsonWithNull.contains("virtualColumns"),
        "virtualColumns field should not appear in JSON when null"
    );
  }

  @Test
  public void testSerde_emptyVirtualColumns_deserializesToEmptyNotNull() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();

    // JSON without virtualColumns field (like old data)
    final String jsonWithoutField = "{\"filter\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"foo\"}}";

    final CompactionTransformSpec deserialized = mapper.readValue(
        jsonWithoutField,
        CompactionTransformSpec.class
    );

    // Should deserialize to VirtualColumns.EMPTY, not null
    Assertions.assertNotNull(deserialized.getVirtualColumns(), "virtualColumns should not be null after deserialization");
    Assertions.assertEquals(VirtualColumns.EMPTY, deserialized.getVirtualColumns(), "virtualColumns should be EMPTY");
    Assertions.assertTrue(deserialized.getVirtualColumns().isEmpty(), "virtualColumns should be empty");
  }
}
