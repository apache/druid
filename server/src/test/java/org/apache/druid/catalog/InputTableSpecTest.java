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

package org.apache.druid.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class InputTableSpecTest
{
  @Test
  public void testMinimalBuilder()
  {
    // Minimum possible definition
    InputSource inputSource = new InlineInputSource("a,b,1\nc,d,2\n");
    InputFormat inputFormat = CatalogTests.csvFormat();
    InputTableSpec spec = InputTableSpec
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", "varchar")
        .build();

    spec.validate();
    assertSame(inputSource, spec.inputSource());
    assertSame(inputFormat, spec.format());
    List<InputColumnSpec> columns = spec.columns();
    assertEquals(1, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals("varchar", columns.get(0).sqlType());

    InputTableSpec copy = spec.toBuilder().build();
    assertEquals(spec, copy);
  }

  @Test
  public void testValidation()
  {
    InputTableSpec spec = InputTableSpec.builder().build();
    try {
      spec.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    InputSource inputSource = new InlineInputSource("a,b,1\nc,d,2\n");
    spec = InputTableSpec
        .builder()
        .source(inputSource)
        .build();
    try {
      spec.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    InputFormat inputFormat = CatalogTests.csvFormat();
    spec = InputTableSpec
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .build();
    try {
      spec.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    try {
      spec = InputTableSpec
          .builder()
          .source(inputSource)
          .format(inputFormat)
          .column(null, "VARCHAR")
          .build();
      spec.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    spec = InputTableSpec
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", null)
        .build();
    try {
      spec.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    spec = InputTableSpec
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", "varchar")
        .column("a", "varchar")
        .build();
    try {
      spec.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }
  }

  @Test
  public void testSerialization()
  {
    ObjectMapper mapper = new ObjectMapper();
    InputSource inputSource = new InlineInputSource("a,b,1\nc,d,2\n");
    InputFormat inputFormat = CatalogTests.csvFormat();
    InputTableSpec spec1 = InputTableSpec
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", "varchar")
        .build();

    // Round-trip
    TableSpec spec2 = TableSpec.fromBytes(mapper, spec1.toBytes(mapper));
    assertEquals(spec1, spec2);

    // Sanity check of toString, which uses JSON
    assertNotNull(spec1.toString());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(InputTableSpec.class)
                  .usingGetClass()
                  .verify();
  }
}
