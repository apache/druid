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
import org.apache.druid.catalog.DatasourceColumnSpec.MeasureSpec;
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
public class DatasourceSpecTest
{
  @Test
  public void testMinimalBuilder()
  {
    // Minimum possible definition
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .build();

    defn.validate();
    assertEquals("PT1D", defn.segmentGranularity());
    assertNull(defn.rollupGranularity());
    assertEquals(0, defn.targetSegmentRows());

    DatasourceSpec copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testFullBuilder()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .build();

    defn.validate();
    assertEquals("PT1H", defn.segmentGranularity());
    assertEquals("PT1M", defn.rollupGranularity());
    assertEquals(1_000_000, defn.targetSegmentRows());

    DatasourceSpec copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testProperties()
  {
    Map<String, Object> props = ImmutableMap.of(
        "foo", 10, "bar", "mumble");
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1M")
        .properties(props)
        .build();

    defn.validate();
    assertEquals(props, defn.properties());

    DatasourceSpec copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testColumns()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1D")
        .rollupGranularity("PT1M")
        .column("a", null)
        .column("b", "VARCHAR")
        .measure("c", "BIGINT", "SUM")
        .build();

    defn.validate();
    List<DatasourceColumnSpec> columns = defn.columns();
    assertEquals(3, columns.size());
    assertTrue(columns.get(0) instanceof DatasourceColumnSpec);
    assertEquals("a", columns.get(0).name());
    assertNull(columns.get(0).sqlType());
    assertTrue(columns.get(1) instanceof DatasourceColumnSpec);
    assertEquals("b", columns.get(1).name());
    assertEquals("VARCHAR", columns.get(1).sqlType());
    assertTrue(columns.get(2) instanceof MeasureSpec);
    assertEquals("c", columns.get(2).name());
    assertEquals("BIGINT", columns.get(2).sqlType());
    assertEquals("SUM", ((MeasureSpec) columns.get(2)).aggregateFn());

    DatasourceSpec copy = defn.toBuilder().build();
    assertEquals(defn, copy);

    try {
      defn = DatasourceSpec.builder()
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
      defn = DatasourceSpec.builder()
          .segmentGranularity("PT1D")
          .measure("c", "BIGINT", "SUM")
          .build();
      defn.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    try {
      defn = DatasourceSpec.builder()
          .segmentGranularity("PT1D")
          .column("a", null)
          .column("a", null)
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
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .build();

    assertNull(defn.rollupGranularity());
    assertEquals("PT1H", defn.segmentGranularity());

    // Negative segment size mapped to 0
    defn = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .targetSegmentRows(-1)
        .build();
    assertEquals(0, defn.targetSegmentRows());
  }

  @Test
  public void testSerialization()
  {
    ObjectMapper mapper = new ObjectMapper();
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .build();

    // Round-trip
    TableSpec defn2 = TableSpec.fromBytes(mapper, defn.toBytes(mapper));
    assertEquals(defn, defn2);

    // Sanity check of toString, which uses JSON
    assertNotNull(defn.toString());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DatasourceSpec.class)
                  .usingGetClass()
                  .verify();
  }
}
