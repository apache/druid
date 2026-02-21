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

package org.apache.druid.msq.input.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.FilterSegmentPruner;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TableInputSpecTest extends InitializedNullHandlingTest
{

  private final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                        .registerModules(new MSQIndexingModule().getJacksonModules());

  @Test
  public void testSerde() throws Exception
  {
    final TableInputSpec spec = new TableInputSpec(
        "myds",
        Collections.singletonList(Intervals.of("2000/P1M")),
        null,
        new FilterSegmentPruner(
            new EqualityFilter("dim", ColumnType.STRING, "val", null),
            Collections.singleton("dim")
        )
    );

    Assert.assertEquals(
        spec,
        mapper.readValue(mapper.writeValueAsString(spec), InputSpec.class)
    );
  }

  @Test
  public void testSerdeEmptyFilterFields() throws Exception
  {
    final TableInputSpec spec = new TableInputSpec(
        "myds",
        Collections.singletonList(Intervals.of("2000/P1M")),
        null,
        new FilterSegmentPruner(
            new EqualityFilter("dim", ColumnType.STRING, "val", null),
            Collections.emptySet()
        )
    );

    Assert.assertEquals(
        spec,
        mapper.readValue(mapper.writeValueAsString(spec), InputSpec.class)
    );
  }

  @Test
  public void testSerdeEternityInterval() throws Exception
  {
    final TableInputSpec spec = new TableInputSpec(
        "myds",
        Intervals.ONLY_ETERNITY,
        null,
        new FilterSegmentPruner(
            new EqualityFilter("dim", ColumnType.STRING, "val", null),
            null
        )
    );

    Assert.assertEquals(
        spec,
        mapper.readValue(mapper.writeValueAsString(spec), InputSpec.class)
    );
  }

  @Test
  public void testSerdeWithSegments() throws Exception
  {
    final TableInputSpec spec = new TableInputSpec(
        "myds",
        Collections.singletonList(Intervals.of("2000/P1M")),
        Collections.singletonList(new SegmentDescriptor(Intervals.of("2000/P1M"), "version", 0)),
        new FilterSegmentPruner(
            new EqualityFilter("dim", ColumnType.STRING, "val", null),
            Collections.singleton("dim")
        )
    );

    Assert.assertEquals(
        spec,
        mapper.readValue(mapper.writeValueAsString(spec), InputSpec.class)
    );
  }

  @Test
  public void testSerdeLegacy() throws Exception
  {
    // missing pruner
    final String legacy = "{\n"
                          + "  \"type\" : \"table\",\n"
                          + "  \"dataSource\" : \"myds\",\n"
                          + "  \"intervals\" : [ \"2000-01-01T00:00:00.000Z/2000-02-01T00:00:00.000Z\" ],\n"
                          + "  \"filter\" : {\n"
                          + "    \"type\" : \"equals\",\n"
                          + "    \"column\" : \"dim\",\n"
                          + "    \"matchValueType\" : \"STRING\",\n"
                          + "    \"matchValue\" : \"val\"\n"
                          + "  },\n"
                          + "  \"filterFields\" : [ \"dim\" ],\n"
                          + "  \"intervalsForSerialization\" : [ \"2000-01-01T00:00:00.000Z/2000-02-01T00:00:00.000Z\" ]\n"
                          + "}";
    final TableInputSpec expected = new TableInputSpec(
        "myds",
        Collections.singletonList(Intervals.of("2000/P1M")),
        null,
        new FilterSegmentPruner(
            new EqualityFilter("dim", ColumnType.STRING, "val", null),
            Collections.singleton("dim")
        )
    );

    Assert.assertEquals(
        expected,
        mapper.readValue(legacy, InputSpec.class)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TableInputSpec.class).usingGetClass().verify();
  }
}
