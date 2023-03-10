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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MapInputRowParserTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final TimestampSpec timestampSpec = new TimestampSpec("time", null, null);
  private final DimensionsSpec dimensionsSpec = DimensionsSpec
      .builder()
      .setDefaultSchemaDimensions(ImmutableList.of("dim"))
      .setDimensionExclusions(ImmutableList.of("time"))
      .build();

  @Test
  public void testParseValidInput()
  {
    final InputRow inputRow = MapInputRowParser.parse(
        timestampSpec,
        dimensionsSpec,
        ImmutableMap.of("time", "2020-01-01", "dim", 0, "met", 10)
    );
    Assert.assertEquals(dimensionsSpec.getDimensionNames(), inputRow.getDimensions());
    Assert.assertEquals(DateTimes.of("2020-01-01"), inputRow.getTimestamp());
    Assert.assertEquals(ImmutableList.of("0"), inputRow.getDimension("dim"));
    Assert.assertEquals(10, inputRow.getMetric("met"));
  }

  @Test
  public void testParseInvalidTimestampThrowParseException()
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Timestamp[invalid timestamp] is unparseable!");
    final InputRow inputRow = MapInputRowParser.parse(
        timestampSpec,
        dimensionsSpec,
        ImmutableMap.of("time", "invalid timestamp", "dim", 0, "met", 10)
    );
  }

  @Test
  public void testParseMissingTimestampThrowParseException()
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Timestamp[null] is unparseable!");
    final InputRow inputRow = MapInputRowParser.parse(
        timestampSpec,
        dimensionsSpec,
        ImmutableMap.of("dim", 0, "met", 10)
    );
  }

  @Test
  public void testParseTimestampSmallerThanMinThrowParseException()
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Encountered row with timestamp[-146136543-09-08T08:23:32.095Z] that cannot be represented as a long");
    MapInputRowParser.parse(
        timestampSpec,
        dimensionsSpec,
        ImmutableMap.of("time", DateTimes.utc(JodaUtils.MIN_INSTANT - 1), "dim", 0, "met", 10)
    );
  }

  @Test
  public void testParseTimestampLargerThanMaxThrowParseException()
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Encountered row with timestamp[146140482-04-24T15:36:27.904Z] that cannot be represented as a long");
    MapInputRowParser.parse(
        timestampSpec,
        dimensionsSpec,
        ImmutableMap.of("time", DateTimes.utc(JodaUtils.MAX_INSTANT + 1), "dim", 0, "met", 10)
    );
  }

  @Test
  public void testIncludeOnlyDimensionsDefinedInDimensionsSpec()
  {
    final InputRow inputRow = MapInputRowParser.parse(
        timestampSpec,
        dimensionsSpec,
        ImmutableMap.of("time", "2020-01-01", "dim", "val1", "dim2", "val2", "met", 10)
    );
    Assert.assertEquals(dimensionsSpec.getDimensionNames(), inputRow.getDimensions());
    Assert.assertEquals(DateTimes.of("2020-01-01"), inputRow.getTimestamp());
    Assert.assertEquals(ImmutableList.of("val1"), inputRow.getDimension("dim"));
    Assert.assertEquals(10, inputRow.getMetric("met"));
  }

  @Test
  public void testIncludeOnlyDiscoveredDimensionsFromInputWhenDimensionsSpecIsEmpty()
  {
    final InputRow inputRow = MapInputRowParser.parse(
        timestampSpec,
        DimensionsSpec.builder().setDimensionExclusions(ImmutableList.of("time", "met")).build(),
        ImmutableMap.of("time", "2020-01-01", "dim", "val1", "dim2", "val2", "met", 10)
    );
    Assert.assertEquals(ImmutableList.of("dim", "dim2"), inputRow.getDimensions());
    Assert.assertEquals(DateTimes.of("2020-01-01"), inputRow.getTimestamp());
    Assert.assertEquals(ImmutableList.of("val1"), inputRow.getDimension("dim"));
    Assert.assertEquals(ImmutableList.of("val2"), inputRow.getDimension("dim2"));
    Assert.assertEquals(10, inputRow.getMetric("met"));
  }

  @Test
  public void testIncludeAllDimensions()
  {
    final InputRow inputRow = MapInputRowParser.parse(
        timestampSpec,
        DimensionsSpec
            .builder()
            .setDefaultSchemaDimensions(ImmutableList.of("dim3"))
            .setDimensionExclusions(ImmutableList.of("time", "met"))
            .setIncludeAllDimensions(true)
            .build(),
        ImmutableMap.of("time", "2020-01-01", "dim", "val1", "dim2", "val2", "dim3", "val3", "met", 10)
    );
    // dim3 should appear first as it's explicitly defined in dimensionsSpec.
    // Discovered dimensions, i.e., dim and dim2, should be in the parsed row since includeAllDimensions is set.
    Assert.assertEquals(ImmutableList.of("dim3", "dim", "dim2"), inputRow.getDimensions());
    Assert.assertEquals(DateTimes.of("2020-01-01"), inputRow.getTimestamp());
    Assert.assertEquals(ImmutableList.of("val3"), inputRow.getDimension("dim3"));
    Assert.assertEquals(ImmutableList.of("val1"), inputRow.getDimension("dim"));
    Assert.assertEquals(ImmutableList.of("val2"), inputRow.getDimension("dim2"));
    Assert.assertEquals(10, inputRow.getMetric("met"));
  }
}
