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

package org.apache.druid.query.select;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SelectQueryTest
{
  private static final String SOME_QUERY_THAT_IS_DOOMED_TO_FAIL =
      "{\n"
      + "   \"queryType\": \"select\",\n"
      + "   \"dataSource\": \"wikipedia\",\n"
      + "   \"descending\": \"false\",\n"
      + "   \"dimensions\":[],\n"
      + "   \"metrics\":[],\n"
      + "   \"granularity\": \"all\",\n"
      + "   \"intervals\": [\n"
      + "     \"2013-01-01/2013-01-02\"\n"
      + "   ],\n"
      + "   \"pagingSpec\":{\"pagingIdentifiers\": {}, \"threshold\":5}\n"
      + " }";

  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws Exception
  {
    final String exceptionMessage =
        StringUtils.format(
            "Cannot construct instance of `org.apache.druid.query.select.SelectQuery`, problem: %s",
            SelectQuery.REMOVED_ERROR_MESSAGE
        );
    expectedException.expect(JsonMappingException.class);
    expectedException.expectMessage(exceptionMessage);
    objectMapper.readValue(SOME_QUERY_THAT_IS_DOOMED_TO_FAIL, SelectQuery.class);
  }
}
