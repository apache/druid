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
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test of validation and serialization of the catalog table definitions.
 */
public class DatasourceDefnTest
{
  @Test
  public void testMinimalBuilder()
  {
    // Minimum possible definition
    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1D")
        .build();

    defn.validate();
    assertEquals("PT1D", defn.segmentGranularity());
    assertNull(defn.rollupGranularity());
    assertEquals(0, defn.targetSegmentRows());

    DatasourceDefn copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testFullBuilder()
  {
    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .build();

    defn.validate();
    assertEquals("PT1H", defn.segmentGranularity());
    assertEquals("PT1M", defn.rollupGranularity());
    assertEquals(1_000_000, defn.targetSegmentRows());

    DatasourceDefn copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testProperties()
  {
    Map<String, Object> props = ImmutableMap.of(
        "foo", 10, "bar", "mumble");
    DatasourceDefn defn = DatasourceDefn.builder()
        .rollupGranularity("PT1M")
        .properties(props)
        .build();

    defn.validate();
    assertEquals(props, defn.properties());

    DatasourceDefn copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testColumns()
  {
    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1D")
        .rollupGranularity("PT1M")
        .column(DatasourceColumnDefn.builder("a").build())
        .column(DatasourceColumnDefn.builder("b").sqlType("VARCHAR").build())
        .column(DatasourceColumnDefn.builder("c").sqlType("BIGINT").measure("SUM").build())
        .build();

    defn.validate();
    List<ColumnDefn> columns = defn.columns();
    assertEquals(3, columns.size());
    assertTrue(columns.get(0) instanceof DatasourceColumnDefn);
    assertEquals("a", columns.get(0).name());
    assertNull(columns.get(0).sqlType());
    assertTrue(columns.get(1) instanceof DatasourceColumnDefn);
    assertEquals("b", columns.get(1).name());
    assertEquals("VARCHAR", columns.get(1).sqlType());
    assertTrue(columns.get(2) instanceof MeasureColumnDefn);
    assertEquals("c", columns.get(2).name());
    assertEquals("BIGINT", columns.get(2).sqlType());
    assertEquals("SUM", ((MeasureColumnDefn) columns.get(2)).aggregateFn());

    DatasourceDefn copy = defn.toBuilder().build();
    assertEquals(defn, copy);

    try {
      defn = DatasourceDefn.builder()
          .segmentGranularity("PT1D")
          .column("c", "FOO")
          .build();
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    try {
      defn = DatasourceDefn.builder()
          .segmentGranularity("PT1D")
          .column(DatasourceColumnDefn.builder("c").sqlType("BIGINT").measure("SUM").build())
          .build();
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    try {
      defn = DatasourceDefn.builder()
          .segmentGranularity("PT1D")
          .column(DatasourceColumnDefn.builder("a").build())
          .column(DatasourceColumnDefn.builder("a").build())
          .build();
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }
  }

  @Test
  public void testValidation()
  {
    // Ignore rollup grain for detail table
    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1H")
        .build();

    assertNull(defn.rollupGranularity());
    assertEquals("PT1H", defn.segmentGranularity());

    // Negative segment size mapped to 0
    defn = DatasourceDefn.builder()
        .segmentGranularity("PT1H")
        .targetSegmentRows(-1)
        .build();
    assertEquals(0, defn.targetSegmentRows());
  }

  @Test
  public void testSerialization()
  {
    ObjectMapper mapper = new ObjectMapper();
    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
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
    EqualsVerifier.forClass(DatasourceDefn.class)
                  .usingGetClass()
                  .verify();
  }
}
