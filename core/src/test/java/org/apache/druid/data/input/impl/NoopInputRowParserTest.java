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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class NoopInputRowParserTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testSerdeWithNullParseSpec() throws Exception
  {
    String jsonStr = "{ \"type\":\"noop\" }";

    InputRowParser actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, InputRowParser.class)
        ),
        InputRowParser.class
    );

    Assert.assertEquals(new NoopInputRowParser(null), actual);
  }

  @Test
  public void testSerdeWithNonNullParseSpec() throws Exception
  {
    String jsonStr = "{"
                     + "\"type\":\"noop\","
                     + "\"parseSpec\":{ \"format\":\"timeAndDims\", \"dimensionsSpec\": { \"dimensions\": [\"host\"] } }"
                     + "}";

    InputRowParser actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, InputRowParser.class)
        ),
        InputRowParser.class
    );

    Assert.assertEquals(
        new NoopInputRowParser(
            new TimeAndDimsParseSpec(
                null,
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null)
            )
        ),
        actual
    );
  }
}
