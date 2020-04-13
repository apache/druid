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

package org.apache.druid.data.input.parquet;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Duplicate of {@link WikiParquetInputTest} but for {@link ParquetReader} instead of Hadoop
 */
public class WikiParquetReaderTest extends BaseParquetReaderTest
{
  @Test
  public void testWiki() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("page", "language", "user", "unpatrolled"))),
        Collections.emptyList()
    );
    InputEntityReader reader = createReader("example/wiki/wiki.parquet", schema, JSONPathSpec.DEFAULT);

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals("Gypsy Danger", rows.get(0).getDimension("page").get(0));
    String s1 = rows.get(0).getDimension("language").get(0);
    String s2 = rows.get(0).getDimension("language").get(1);
    Assert.assertEquals("en", s1);
    Assert.assertEquals("zh", s2);

    reader = createReader("example/wiki/wiki.parquet", schema, JSONPathSpec.DEFAULT);
    List<InputRowListPlusRawValues> sampled = sampleAllRows(reader);

    final String expectedJson = "{\n"
                                + "  \"continent\" : \"North America\",\n"
                                + "  \"country\" : \"United States\",\n"
                                + "  \"added\" : 57,\n"
                                + "  \"city\" : \"San Francisco\",\n"
                                + "  \"unpatrolled\" : \"true\",\n"
                                + "  \"delta\" : -143,\n"
                                + "  \"language\" : [ \"en\", \"zh\" ],\n"
                                + "  \"robot\" : \"false\",\n"
                                + "  \"deleted\" : 200,\n"
                                + "  \"newPage\" : \"true\",\n"
                                + "  \"namespace\" : \"article\",\n"
                                + "  \"anonymous\" : \"false\",\n"
                                + "  \"page\" : \"Gypsy Danger\",\n"
                                + "  \"region\" : \"Bay Area\",\n"
                                + "  \"user\" : \"nuclear\",\n"
                                + "  \"timestamp\" : \"2013-08-31T01:02:33Z\"\n"
                                + "}";
    Assert.assertEquals(expectedJson, DEFAULT_JSON_WRITER.writeValueAsString(sampled.get(0).getRawValues()));
  }
}
