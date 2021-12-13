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

package org.apache.druid.java.util.common;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.input.ReaderInputStream;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class JsonUtilsTest
{
  @Test
  public void testSkipValue() throws IOException
  {
    String input = "{"
        + "\"scalar\": 10,"
        + "\"map\": {\"foo\": 10, \"bar\": null},"
        + "\"array\": [10, 20]"
        + "}";
    ObjectMapper objectMapper = new ObjectMapper();
    JsonParser jp = objectMapper.getFactory().createParser(
        new ReaderInputStream(new StringReader(input), StandardCharsets.UTF_8));
    jp.nextToken();
    jp.nextToken();
    assertEquals(JsonToken.FIELD_NAME, jp.currentToken());
    jp.nextToken();
    JsonUtils.skipValue(jp, "scalar");
    assertEquals(JsonToken.FIELD_NAME, jp.currentToken());
    jp.nextToken();
    JsonUtils.skipValue(jp, "map");
    assertEquals(JsonToken.FIELD_NAME, jp.currentToken());
    jp.nextToken();
    JsonUtils.skipValue(jp, "array");
    assertEquals(JsonToken.END_OBJECT, jp.currentToken());
  }
}
