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

package org.apache.druid.indexing.overlord.sampler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.logging.log4j.util.Strings;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class InputSourceSamplerDiscoveryTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final List<String> STR_JSON_ROWS = ImmutableList.of(
      "{ \"t\": \"2019-04-22T12:00\", \"string\": \"a\", \"long\": 1, \"double\":1.1, \"bool\":true, \"variant\":\"a\", \"array\":[1, 2, 3], \"nested\": {\"x\":1, \"y\": 2.0}}",
      "{ \"t\": \"2019-04-22T12:00\", \"string\": \"b\", \"long\": 2, \"double\":2.2, \"bool\":false, \"variant\": 1.0, \"array\":[4, 5, 6], \"nested\": {\"x\":2, \"y\": 4.0} }",
      "{ \"t\": \"2019-04-22T12:01\", \"string\": null, \"long\": null, \"double\":3.3, \"bool\":null, \"variant\":2, \"array\":[7, 8, 9], \"nested\": {\"x\":3, \"y\": 6.0} }",
      "{ \"t\": \"2019-04-22T12:00\", \"string\": \"c\", \"long\": 4, \"double\":4.4, \"bool\":true, \"variant\":\"3\", \"array\":[10, 11, 12], \"nested\": {\"x\":4, \"y\": 8.0} }",
      "{ \"t\": \"2019-04-22T12:00\", \"string\": \"d\", \"long\": 5, \"double\":null, \"bool\":false, \"variant\":null, \"array\":[13, 14, 15], \"nested\": {\"x\":5, \"y\": 10.0} }",
      "{ \"t\": \"bad_timestamp\", \"string\": \"e\", \"long\": 6, \"double\":6.6, \"bool\":true, \"variant\":\"4\", \"array\":[16, 17, 18], \"nested\": {\"x\":6, \"y\": 12.0} }"
  );
  private InputSourceSampler inputSourceSampler = new InputSourceSampler(OBJECT_MAPPER);

  @Test
  public void testDiscoveredTypesNonStrictBooleans()
  {

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      final InputSource inputSource = new InlineInputSource(Strings.join(STR_JSON_ROWS, '\n'));
      final SamplerResponse response = inputSourceSampler.sample(
          inputSource,
          new JsonInputFormat(null, null, null, null, null),
          new DataSchema(
              "test",
              new TimestampSpec("t", null, null),
              DimensionsSpec.builder().useSchemaDiscovery(true).build(),
              null,
              null,
              null
          ),
          null
      );

      Assert.assertEquals(6, response.getNumRowsRead());
      Assert.assertEquals(5, response.getNumRowsIndexed());
      Assert.assertEquals(6, response.getData().size());
      Assert.assertEquals(
          ImmutableList.of(
              new StringDimensionSchema("string"),
              new LongDimensionSchema("long"),
              new DoubleDimensionSchema("double"),
              new StringDimensionSchema("bool"),
              new StringDimensionSchema("variant"),
              new AutoTypeColumnSchema("array", null),
              new AutoTypeColumnSchema("nested", null)
          ),
          response.getLogicalDimensions()
      );

      Assert.assertEquals(
          ImmutableList.of(
              new AutoTypeColumnSchema("string", null),
              new AutoTypeColumnSchema("long", null),
              new AutoTypeColumnSchema("double", null),
              new AutoTypeColumnSchema("bool", null),
              new AutoTypeColumnSchema("variant", null),
              new AutoTypeColumnSchema("array", null),
              new AutoTypeColumnSchema("nested", null)
          ),
          response.getPhysicalDimensions()
      );
      Assert.assertEquals(
          RowSignature.builder()
                      .addTimeColumn()
                      .add("string", ColumnType.STRING)
                      .add("long", ColumnType.LONG)
                      .add("double", ColumnType.DOUBLE)
                      .add("bool", ColumnType.STRING)
                      .add("variant", ColumnType.STRING)
                      .add("array", ColumnType.LONG_ARRAY)
                      .add("nested", ColumnType.NESTED_DATA)
                      .build(),
          response.getLogicalSegmentSchema()
      );
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
  }

  @Test
  public void testDiscoveredTypesStrictBooleans()
  {
    final InputSource inputSource = new InlineInputSource(Strings.join(STR_JSON_ROWS, '\n'));
    final SamplerResponse response = inputSourceSampler.sample(
        inputSource,
        new JsonInputFormat(null, null, null, null, null),
        new DataSchema(
            "test",
            new TimestampSpec("t", null, null),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            null,
            null,
            null
        ),
        null
    );

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());
    Assert.assertEquals(
        ImmutableList.of(
            new StringDimensionSchema("string"),
            new LongDimensionSchema("long"),
            new DoubleDimensionSchema("double"),
            new LongDimensionSchema("bool"),
            new StringDimensionSchema("variant"),
            new AutoTypeColumnSchema("array", null),
            new AutoTypeColumnSchema("nested", null)
        ),
        response.getLogicalDimensions()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new AutoTypeColumnSchema("string", null),
            new AutoTypeColumnSchema("long", null),
            new AutoTypeColumnSchema("double", null),
            new AutoTypeColumnSchema("bool", null),
            new AutoTypeColumnSchema("variant", null),
            new AutoTypeColumnSchema("array", null),
            new AutoTypeColumnSchema("nested", null)
        ),
        response.getPhysicalDimensions()
    );
    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("string", ColumnType.STRING)
                    .add("long", ColumnType.LONG)
                    .add("double", ColumnType.DOUBLE)
                    .add("bool", ColumnType.LONG)
                    .add("variant", ColumnType.STRING)
                    .add("array", ColumnType.LONG_ARRAY)
                    .add("nested", ColumnType.NESTED_DATA)
                    .build(),
        response.getLogicalSegmentSchema()
    );
  }

  @Test
  public void testTypesClassicDiscovery()
  {
    final InputSource inputSource = new InlineInputSource(Strings.join(STR_JSON_ROWS, '\n'));
    final DataSchema dataSchema = new DataSchema(
        "test",
        new TimestampSpec("t", null, null),
        DimensionsSpec.builder().build(),
        null,
        null,
        null
    );
    final SamplerResponse response = inputSourceSampler.sample(
        inputSource,
        new JsonInputFormat(null, null, null, null, null),
        dataSchema,
        null
    );

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());
    Assert.assertEquals(
        ImmutableList.of(
            new StringDimensionSchema("string"),
            new StringDimensionSchema("long"),
            new StringDimensionSchema("double"),
            new StringDimensionSchema("bool"),
            new StringDimensionSchema("variant"),
            new StringDimensionSchema("array")
        ),
        response.getLogicalDimensions()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new StringDimensionSchema("string"),
            new StringDimensionSchema("long"),
            new StringDimensionSchema("double"),
            new StringDimensionSchema("bool"),
            new StringDimensionSchema("variant"),
            new StringDimensionSchema("array")
        ),
        response.getPhysicalDimensions()
    );
    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("string", ColumnType.STRING)
                    .add("long", ColumnType.STRING)
                    .add("double", ColumnType.STRING)
                    .add("bool", ColumnType.STRING)
                    .add("variant", ColumnType.STRING)
                    .add("array", ColumnType.STRING)
                    .build(),
        response.getLogicalSegmentSchema()
    );
  }

  @Test
  public void testTypesNoDiscoveryExplicitSchema()
  {
    final InputSource inputSource = new InlineInputSource(Strings.join(STR_JSON_ROWS, '\n'));
    final DataSchema dataSchema = new DataSchema(
        "test",
        new TimestampSpec("t", null, null),
        DimensionsSpec.builder().setDimensions(
            ImmutableList.of(new StringDimensionSchema("string"),
                             new LongDimensionSchema("long"),
                             new DoubleDimensionSchema("double"),
                             new StringDimensionSchema("bool"),
                             new AutoTypeColumnSchema("variant", null),
                             new AutoTypeColumnSchema("array", null),
                             new AutoTypeColumnSchema("nested", null)
            )
        ).build(),
        null,
        null,
        null
    );
    final SamplerResponse response = inputSourceSampler.sample(
        inputSource,
        new JsonInputFormat(null, null, null, null, null),
        dataSchema,
        null
    );

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());
    Assert.assertEquals(
        dataSchema.getDimensionsSpec().getDimensions(),
        response.getLogicalDimensions()
    );

    Assert.assertEquals(
        dataSchema.getDimensionsSpec().getDimensions(),
        response.getPhysicalDimensions()
    );
    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("string", ColumnType.STRING)
                    .add("long", ColumnType.LONG)
                    .add("double", ColumnType.DOUBLE)
                    .add("bool", ColumnType.STRING)
                    .add("variant", ColumnType.STRING)
                    .add("array", ColumnType.LONG_ARRAY)
                    .add("nested", ColumnType.NESTED_DATA)
                    .build(),
        response.getLogicalSegmentSchema()
    );
  }
}
