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

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.MatchingDimExtractionFn;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.extraction.StrlenExtractionFn;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

public class ExtractionDimensionSpecTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final String oldJson = "{\n"
                           + "    \"type\": \"extraction\",\n"
                           + "    \"outputName\": \"first3Letters\",\n"
                           + "    \"dimension\": \"myDim\","
                           + "    \"extractionFn\": {\n"
                           + "        \"type\": \"regex\",\n"
                           + "        \"expr\": \"(...).*\"\n"
                           + "    }\n"
                           + "}";

    final ExtractionDimensionSpec extractionDimensionSpec = (ExtractionDimensionSpec) objectMapper.readValue(oldJson, DimensionSpec.class);

    Assert.assertEquals("first3Letters", extractionDimensionSpec.getOutputName());
    Assert.assertEquals("myDim", extractionDimensionSpec.getDimension());
    Assert.assertNotNull(extractionDimensionSpec.getExtractionFn());
    Assert.assertEquals(ValueType.STRING, extractionDimensionSpec.getOutputType());
    Assert.assertTrue(extractionDimensionSpec.getExtractionFn() instanceof RegexDimExtractionFn);

    Assert.assertEquals(
        extractionDimensionSpec,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionDimensionSpec),
            DimensionSpec.class
        )
    );
  }

  @Test
  public void testSerdeWithType() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final String oldJson = "{\n"
                           + "    \"type\": \"extraction\",\n"
                           + "    \"outputName\": \"first3Letters\",\n"
                           + "    \"outputType\": \"LONG\",\n"
                           + "    \"dimension\": \"myDim\","
                           + "    \"extractionFn\": {\n"
                           + "        \"type\": \"regex\",\n"
                           + "        \"expr\": \"(...).*\"\n"
                           + "    }\n"
                           + "}";

    final ExtractionDimensionSpec extractionDimensionSpec = (ExtractionDimensionSpec) objectMapper.readValue(oldJson, DimensionSpec.class);

    Assert.assertEquals("first3Letters", extractionDimensionSpec.getOutputName());
    Assert.assertEquals("myDim", extractionDimensionSpec.getDimension());
    Assert.assertNotNull(extractionDimensionSpec.getExtractionFn());
    Assert.assertEquals(ValueType.LONG, extractionDimensionSpec.getOutputType());
    Assert.assertTrue(extractionDimensionSpec.getExtractionFn() instanceof RegexDimExtractionFn);

    Assert.assertEquals(
        extractionDimensionSpec,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionDimensionSpec),
            DimensionSpec.class
        )
    );
  }

  @Test
  public void testSerdeBackwardsCompatibility() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final String oldJson = "{\n"
                        + "    \"type\": \"extraction\",\n"
                        + "    \"outputName\": \"first3Letters\",\n"
                        + "    \"dimension\": \"myDim\","
                        + "    \"dimExtractionFn\": {\n"
                        + "        \"type\": \"regex\",\n"
                        + "        \"expr\": \"(...).*\"\n"
                        + "    }\n"
                        + "}";

    final ExtractionDimensionSpec extractionDimensionSpec = (ExtractionDimensionSpec) objectMapper.readValue(oldJson, DimensionSpec.class);

    Assert.assertEquals("first3Letters", extractionDimensionSpec.getOutputName());
    Assert.assertEquals("myDim", extractionDimensionSpec.getDimension());
    Assert.assertNotNull(extractionDimensionSpec.getExtractionFn());
    Assert.assertTrue(extractionDimensionSpec.getExtractionFn() instanceof RegexDimExtractionFn);

    Assert.assertEquals(
        extractionDimensionSpec,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionDimensionSpec),
            DimensionSpec.class
        )
    );

    // new trumps old
    final String oldAndNewJson = "{\n"
                           + "    \"type\": \"extraction\",\n"
                           + "    \"outputName\": \"first3Letters\",\n"
                           + "    \"dimension\": \"myDim\","
                           + "    \"extractionFn\": {\n"
                           + "        \"type\": \"partial\",\n"
                           + "        \"expr\": \"(...).*\"\n"
                           + "    },\n"
                           + "    \"dimExtractionFn\": {\n"
                           + "        \"type\": \"regex\",\n"
                           + "        \"expr\": \"(...).*\"\n"
                           + "    }\n"
                           + "}";

    Assert.assertTrue(
        objectMapper.readValue(oldAndNewJson, DimensionSpec.class)
                    .getExtractionFn() instanceof MatchingDimExtractionFn
    );
  }

  @Test
  public void testCacheKey()
  {
    final ExtractionDimensionSpec dimensionSpec = new ExtractionDimensionSpec(
        "foo",
        "len",
        ValueType.LONG,
        StrlenExtractionFn.instance()
    );
    final byte[] expected = new byte[]{1, 7, 102, 111, 111, 9, 14, 7, 76, 79, 78, 71};
    Assert.assertArrayEquals(expected, dimensionSpec.getCacheKey());
  }
}
