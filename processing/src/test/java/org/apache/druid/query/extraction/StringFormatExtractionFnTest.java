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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 *
 */
public class StringFormatExtractionFnTest
{

  @Test
  public void testApply()
  {
    StringFormatExtractionFn fn = new StringFormatExtractionFn("[%s]");
    long test = 1000L;
    Assertions.assertEquals("[1000]", fn.apply(test));
  }

  @Test
  public void testApplyNull1()
  {
    String test = null;
    Assertions.assertEquals("[null]", format("[%s]", "nullString").apply(test));
    Assertions.assertEquals("[]", format("[%s]", "emptyString").apply(test));
    Assertions.assertNull(format("[%s]", "returnNull").apply(test));
  }

  @Test
  public void testApplyNull2()
  {
    String test = null;
    Assertions.assertEquals("null", format("%s", "nullString").apply(test));
    Assertions.assertEquals(
        "",
        format("%s", "emptyString").apply(test)
    );
    Assertions.assertNull(format("%s", "returnNull").apply(test));
  }

  @Test
  public void testInvalidOption1()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new StringFormatExtractionFn(""));
  }

  @Test
  public void testSerde() throws Exception
  {
    validateSerde("{ \"type\" : \"stringFormat\", \"format\" : \"[%s]\" }");
    validateSerde(
        "{ \"type\" : \"stringFormat\", \"format\" : \"[%s]\", \"nullHandling\" : \"returnNull\" }"
    );
  }

  @Test
  public void testInvalidOption2()
  {
    Assertions.assertThrows(
        JsonMappingException.class,
        () -> validateSerde(
            "{ \"type\" : \"stringFormat\", \"format\" : \"[%s]\", \"nullHandling\" : \"invalid\" }"
        )
    );
  }

  public StringFormatExtractionFn format(String format, String nullHandling)
  {
    return new StringFormatExtractionFn(format, StringFormatExtractionFn.NullHandling.forValue(nullHandling));
  }

  private void validateSerde(String json) throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    StringFormatExtractionFn extractionFn = (StringFormatExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assertions.assertEquals("[%s]", extractionFn.getFormat());

    // round trip
    Assertions.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }
}
