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

package org.apache.druid.jackson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StrictIntegerDeserializerTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testAcceptsIntegers() throws Exception
  {
    final Holder holder = mapper.readValue("{\"ids\":[0, 7, 2147483647]}", Holder.class);

    Assertions.assertEquals(List.of(0, 7, Integer.MAX_VALUE), holder.ids);
  }

  @Test
  public void testAcceptsStringsHoldingIntegers() throws Exception
  {
    final Holder holder = mapper.readValue("{\"ids\":[\"1\", \"-2\"]}", Holder.class);

    Assertions.assertEquals(List.of(1, -2), holder.ids);
  }

  @Test
  public void testRejectsNonIntegersAndReportsTheValue()
  {
    assertRejected("0.5", "VALUE_NUMBER_FLOAT [0.5]");
    assertRejected("1.0", "VALUE_NUMBER_FLOAT [1.0]");
    assertRejected("true", "VALUE_TRUE [true]");
    assertRejected("\"abc\"", "VALUE_STRING [abc]");
    assertRejected("\"0.5\"", "VALUE_STRING [0.5]");
    assertRejected("\"\"", "VALUE_STRING []");
  }

  @Test
  public void testRejectsOutOfRangeIntegers()
  {
    Assertions.assertThrows(JsonMappingException.class, () -> mapper.readValue("{\"ids\":[2147483648]}", Holder.class));
    assertRejected("\"2147483648\"", "VALUE_STRING [2147483648]");
  }

  private void assertRejected(String json, String expectedValueDescription)
  {
    final JsonMappingException e = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue("{\"ids\":[" + json + "]}", Holder.class)
    );
    Assertions.assertTrue(e.getMessage().contains("Expected a JSON integer but got " + expectedValueDescription), e.getMessage());
  }

  private static class Holder
  {
    private final List<Integer> ids;

    @JsonCreator
    Holder(@JsonProperty("ids") @JsonDeserialize(contentUsing = StrictIntegerDeserializer.class) List<Integer> ids)
    {
      this.ids = ids;
    }
  }
}
