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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class RowSignatureTest
{
  @Test
  public void test_add_withConflict()
  {
    final RowSignature.Builder builder =
        RowSignature.builder()
                    .add("s", ColumnType.STRING)
                    .add("d", ColumnType.DOUBLE)
                    .add("d", ColumnType.LONG);

    Assert.assertThrows(
        "Column [d] has conflicting types",
        IllegalArgumentException.class,
        builder::build
    );
  }

  @Test
  public void test_addAll()
  {
    final RowSignature expectedSignature =
        RowSignature.builder()
                    .add("s", ColumnType.STRING)
                    .add("d", ColumnType.DOUBLE)
                    .add("l", ColumnType.LONG)
                    .build();

    final RowSignature signature =
        RowSignature.builder()
                    .addAll(RowSignature.builder().add("s", ColumnType.STRING).add("d", ColumnType.DOUBLE).build())
                    .addAll(RowSignature.builder().add("l", ColumnType.LONG).build())
                    .build();

    Assert.assertEquals(expectedSignature, signature);
  }

  @Test
  public void test_addAll_withOverlap()
  {
    final RowSignature expectedSignature =
        RowSignature.builder()
                    .add("s", ColumnType.STRING)
                    .add("d", ColumnType.DOUBLE)
                    .add("d", ColumnType.DOUBLE)
                    .build();

    final RowSignature signature =
        RowSignature.builder()
                    .addAll(RowSignature.builder().add("s", ColumnType.STRING).add("d", ColumnType.DOUBLE).build())
                    .addAll(RowSignature.builder().add("d", ColumnType.DOUBLE).build())
                    .build();

    Assert.assertEquals(ImmutableList.of("s", "d", "d"), expectedSignature.getColumnNames());
    Assert.assertEquals(expectedSignature, signature);
  }

  @Test
  public void test_json() throws IOException
  {
    final String signatureString =
        "[{\"name\":\"s\",\"type\":\"STRING\"},"
        + "{\"name\":\"d\",\"type\":\"DOUBLE\"},"
        + "{\"name\":\"f\",\"type\":\"FLOAT\"},"
        + "{\"name\":\"l\",\"type\":\"LONG\"},"
        + "{\"name\":\"u\"},"
        + "{\"name\":\"c\",\"type\":\"COMPLEX\"},"
        + "{\"name\":\"cf\",\"type\":\"COMPLEX<foo>\"},"
        + "{\"name\":\"as\",\"type\":\"ARRAY<STRING>\"}"
        + "]";

    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final RowSignature signature = mapper.readValue(signatureString, RowSignature.class);
    Assert.assertEquals(signatureString, mapper.writeValueAsString(signature));
    Assert.assertEquals(
        RowSignature.builder()
                    .add("s", ColumnType.STRING)
                    .add("d", ColumnType.DOUBLE)
                    .add("f", ColumnType.FLOAT)
                    .add("l", ColumnType.LONG)
                    .add("u", null)
                    .add("c", ColumnType.UNKNOWN_COMPLEX)
                    .add("cf", ColumnType.ofComplex("foo"))
                    .add("as", ColumnType.ofArray(ColumnType.STRING))
                    .build(),
        signature
    );
  }

  @Test
  public void test_json_missingName()
  {
    final String signatureString =
        "[{\"name\":\"s\",\"type\":\"STRING\"},"
        + "{\"type\":\"DOUBLE\"}]";

    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    Assert.assertThrows(
        "Column name must be non-empty",
        IOException.class,
        () -> mapper.readValue(signatureString, RowSignature.class)
    );
  }
}
