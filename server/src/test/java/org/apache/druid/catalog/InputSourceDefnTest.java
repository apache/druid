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

public class InputSourceDefnTest
{
  @Test
  public void testMinimalBuilder()
  {
    // Minimum possible definition
    InputSource inputSource = new InlineInputSource("a,b,1\nc,d,2\n");
    InputFormat inputFormat = CatalogTests.csvFormat();
    InputSourceDefn defn = InputSourceDefn
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", "varchar")
        .build();

    defn.validate();
    assertSame(inputSource, defn.inputSource());
    assertSame(inputFormat, defn.format());
    List<InputColumnDefn> columns = defn.columns();
    assertEquals(1, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals("varchar", columns.get(0).sqlType());

    InputSourceDefn copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testValidation()
  {
    InputSourceDefn defn = InputSourceDefn.builder().build();
    try {
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    InputSource inputSource = new InlineInputSource("a,b,1\nc,d,2\n");
    defn = InputSourceDefn
        .builder()
        .source(inputSource)
        .build();
    try {
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    InputFormat inputFormat = CatalogTests.csvFormat();
    defn = InputSourceDefn
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .build();
    try {
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    try {
      defn = InputSourceDefn
          .builder()
          .source(inputSource)
          .format(inputFormat)
          .column(null, "VARCHAR")
          .build();
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    defn = InputSourceDefn
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", null)
        .build();
    try {
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    defn = InputSourceDefn
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", "varchar")
        .column("a", "varchar")
        .build();
    try {
      defn.validate();
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
    InputSourceDefn defn = InputSourceDefn
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", "varchar")
        .build();

    // Round-trip
    TableDefn defn2 = TableDefn.fromBytes(mapper, defn.toBytes(mapper));
    assertEquals(defn, defn2);

    // Sanity check of toString, which uses JSON
    assertNotNull(defn.toString());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(InputSourceDefn.class)
                  .usingGetClass()
                  .verify();
  }
}
