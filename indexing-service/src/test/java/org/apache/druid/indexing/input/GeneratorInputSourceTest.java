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

package org.apache.druid.indexing.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.junit.Assert;
import org.junit.Test;

public class GeneratorInputSourceTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    GeneratorInputSource inputSource = new GeneratorInputSource(
        "basic",
        null,
        1000,
        2,
        1024L,
        DateTimes.nowUtc().getMillis(),
        1000,
        1.0
    );

    String serialized = MAPPER.writeValueAsString(inputSource);
    GeneratorInputSource sauce = MAPPER.readValue(serialized, GeneratorInputSource.class);

    Assert.assertEquals(inputSource, sauce);
  }

  @Test
  public void testSerdeWithSchema() throws JsonProcessingException
  {
    GeneratorInputSource inputSource = new GeneratorInputSource(
        null,
        ImmutableList.of(
            GeneratorColumnSchema.makeLazyZipf(
                "test",
                ValueType.LONG,
                false,
                1,
                0.0,
                0,
                1000,
                1.3
            )
        ),
        1000,
        2,
        1024L,
        DateTimes.nowUtc().getMillis(),
        1000,
        1.0
    );

    String serialized = MAPPER.writeValueAsString(inputSource);
    GeneratorInputSource sauce = MAPPER.readValue(serialized, GeneratorInputSource.class);

    Assert.assertEquals(inputSource, sauce);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(GeneratorInputSource.class).usingGetClass().verify();
  }
}