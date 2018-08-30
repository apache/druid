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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.InputRowParser;
import org.junit.Assert;
import org.junit.Test;


/**
 */
public class HadoopyStringInputRowParserTest
{
  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{"
                     + "\"type\":\"hadoopyString\","
                     + "\"parseSpec\":{\"format\":\"json\",\"timestampSpec\":{\"column\":\"xXx\"},\"dimensionsSpec\":{}}"
                     + "}";

    ObjectMapper jsonMapper = HadoopDruidIndexerConfig.JSON_MAPPER;
    InputRowParser parser = jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            jsonMapper.readValue(jsonStr, InputRowParser.class)
        ),
        InputRowParser.class
    );

    Assert.assertTrue(parser instanceof HadoopyStringInputRowParser);
    Assert.assertEquals("xXx", parser.getParseSpec().getTimestampSpec().getTimestampColumn());
  }
}
