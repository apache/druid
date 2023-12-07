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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SamplerResponseTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws IOException
  {
    List<SamplerResponse.SamplerResponseRow> data = ImmutableList.of(
        new SamplerResponse.SamplerResponseRow(
            ImmutableMap.of("row1", "val1"),
            ImmutableMap.of("t", 123456, "dim1", "foo", "met1", 6),
            null,
            null
        ),
        new SamplerResponse.SamplerResponseRow(
            ImmutableMap.of("row2", "val2"),
            ImmutableMap.of("t", 123457, "dim1", "foo2", "met1", 7),
            null,
            null
        ),
        new SamplerResponse.SamplerResponseRow(ImmutableMap.of("row3", "val3"), null, true, "Could not parse")
    );

    String out = MAPPER.writeValueAsString(
        new SamplerResponse(
            1123,
            1112,
            ImmutableList.of(
                new StringDimensionSchema("dim1")
            ),
            ImmutableList.of(
                new AutoTypeColumnSchema("dim1", null)
            ),
            RowSignature.builder().addTimeColumn().add("dim1", ColumnType.STRING).add("met1", ColumnType.LONG).build(),
            data
        )
    );
    String expected = "{\"numRowsRead\":1123,\"numRowsIndexed\":1112,\"logicalDimensions\":[{\"type\":\"string\",\"name\":\"dim1\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true}],\"physicalDimensions\":[{\"type\":\"auto\",\"name\":\"dim1\",\"multiValueHandling\":\"SORTED_ARRAY\",\"createBitmapIndex\":true}],\"logicalSegmentSchema\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"dim1\",\"type\":\"STRING\"},{\"name\":\"met1\",\"type\":\"LONG\"}],\"data\":[{\"input\":{\"row1\":\"val1\"},\"parsed\":{\"t\":123456,\"dim1\":\"foo\",\"met1\":6}},{\"input\":{\"row2\":\"val2\"},\"parsed\":{\"t\":123457,\"dim1\":\"foo2\",\"met1\":7}},{\"input\":{\"row3\":\"val3\"},\"unparseable\":true,\"error\":\"Could not parse\"}]}";

    Assert.assertEquals(expected, out);
  }
}
