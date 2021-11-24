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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class RowSignatureTest
{

  @Test
  public void testRowSignatureSerializationToJson() throws JsonProcessingException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("foo1", ColumnType.DOUBLE)
                                            .add("foo2", ColumnType.STRING)
                                            .add("foo3", ColumnType.FLOAT)
                                            .add("foo4", ColumnType.LONG)
                                            .add("foo5", ColumnType.UNKNOWN_COMPLEX)
                                            .add("foo6", ColumnType.ofComplex("bar"))
                                            .add("foo7", ColumnType.STRING_ARRAY)
                                            .add("foo8", ColumnType.LONG_ARRAY)
                                            .add("foo9", ColumnType.DOUBLE_ARRAY)
                                            .build();
    String expectedSerialization = "["
                                   + "{\"name\":\"foo1\",\"type\":\"DOUBLE\"},"
                                   + "{\"name\":\"foo2\",\"type\":\"STRING\"},"
                                   + "{\"name\":\"foo3\",\"type\":\"FLOAT\"},"
                                   + "{\"name\":\"foo4\",\"type\":\"LONG\"},"
                                   + "{\"name\":\"foo5\",\"type\":\"COMPLEX\"},"
                                   + "{\"name\":\"foo6\",\"type\":\"COMPLEX<bar>\"},"
                                   + "{\"name\":\"foo7\",\"type\":\"ARRAY<STRING>\"},"
                                   + "{\"name\":\"foo8\",\"type\":\"ARRAY<LONG>\"},"
                                   + "{\"name\":\"foo9\",\"type\":\"ARRAY<DOUBLE>\"}"
                                   + "]";
    String rowSignatureSerialized = new ObjectMapper().writeValueAsString(rowSignature);
    Assert.assertEquals(expectedSerialization, rowSignatureSerialized);
  }
}
