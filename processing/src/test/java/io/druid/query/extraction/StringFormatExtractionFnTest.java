/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class StringFormatExtractionFnTest
{

  @Test
  public void testApply() throws Exception
  {
    StringFormatExtractionFn fn = new StringFormatExtractionFn("[%s]");
    long test = 1000L;
    Assert.assertEquals("[1000]", fn.apply(test));
  }

  @Test
  public void testApplyNull() throws Exception
  {
    StringFormatExtractionFn fn = new StringFormatExtractionFn("[%s]");
    String test = null;
    Assert.assertEquals("[null]", fn.apply(test));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"stringFormat\", \"format\" : \"[%s]\" }";
    StringFormatExtractionFn extractionFn = (StringFormatExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals("[%s]", extractionFn.getFormat());

    // round trip
    Assert.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }
}
