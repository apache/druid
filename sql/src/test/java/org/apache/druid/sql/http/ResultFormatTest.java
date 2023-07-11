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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.annotations.UsedByJUnitParamsRunner;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.EnumSet;

@RunWith(JUnitParamsRunner.class)
public class ResultFormatTest
{

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  @Parameters(source = ResultFormatTypeProvider.class)
  public void testSerde(ResultFormat target) throws JsonProcessingException
  {
    final String json = jsonMapper.writeValueAsString(target);
    Assert.assertEquals(StringUtils.format("\"%s\"", target.toString()), json);
    Assert.assertEquals(target, jsonMapper.readValue(json, ResultFormat.class));
  }

  @Test
  public void testDeserializeWithDifferentCase() throws JsonProcessingException
  {
    Assert.assertEquals(ResultFormat.OBJECTLINES, jsonMapper.readValue("\"OBJECTLINES\"", ResultFormat.class));
    Assert.assertEquals(ResultFormat.OBJECTLINES, jsonMapper.readValue("\"objectLines\"", ResultFormat.class));
    Assert.assertEquals(ResultFormat.OBJECTLINES, jsonMapper.readValue("\"objectlines\"", ResultFormat.class));
    Assert.assertEquals(ResultFormat.OBJECTLINES, jsonMapper.readValue("\"oBjEcTlInEs\"", ResultFormat.class));
  }

  public static class ResultFormatTypeProvider
  {
    @UsedByJUnitParamsRunner
    public static Object[] provideResultFormats()
    {
      return EnumSet.allOf(ResultFormat.class)
                    .stream()
                    .map(format -> new Object[]{format})
                    .toArray(Object[]::new);
    }
  }
}
