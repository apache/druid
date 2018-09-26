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

package org.apache.druid.emitter.opentsdb;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OpentsdbEventTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper()));
  }

  @Test
  public void testSerDeserOpentsdbEvent() throws Exception
  {
    Map<String, Object> tags = new HashMap<>();
    tags.put("foo", "bar");
    tags.put("baz", 1);
    OpentsdbEvent opentsdbEvent = new OpentsdbEvent("foo.bar", 1000L, 20, tags);
    String opentsdbString = mapper.writeValueAsString(opentsdbEvent);
    OpentsdbEvent expectedOpentsdbEvent = mapper.readerFor(OpentsdbEvent.class)
                                                .readValue(opentsdbString);
    Assert.assertEquals(expectedOpentsdbEvent, opentsdbEvent);
  }
}
