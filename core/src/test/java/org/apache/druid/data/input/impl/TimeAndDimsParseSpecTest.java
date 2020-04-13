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
import junit.framework.Assert;
import org.junit.Test;

/**
 */
public class TimeAndDimsParseSpecTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testSerdeWithNulls() throws Exception
  {
    String jsonStr = "{ \"format\":\"timeAndDims\" }";

    ParseSpec actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, ParseSpec.class)
        ),
        ParseSpec.class
    );

    Assert.assertEquals(new TimeAndDimsParseSpec(null, null), actual);
  }

  @Test
  public void testSerdeWithNonNulls() throws Exception
  {
    String jsonStr = "{"
                     + "\"format\":\"timeAndDims\","
                     + "\"timestampSpec\": { \"column\": \"tcol\" },"
                     + "\"dimensionsSpec\": { \"dimensions\": [\"host\"] }"
                     + "}";

    ParseSpec actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, ParseSpec.class)
        ),
        ParseSpec.class
    );

    Assert.assertEquals(
        new TimeAndDimsParseSpec(
            new TimestampSpec("tcol", null, null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null)
        ),
        actual
    );
  }
}
