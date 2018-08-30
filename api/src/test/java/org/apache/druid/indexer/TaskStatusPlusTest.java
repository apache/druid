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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TaskStatusPlusTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(
        new SimpleModule()
            .addDeserializer(DateTime.class, new DateTimeDeserializer())
            .addSerializer(DateTime.class, ToStringSerializer.instance)
    );
    final TaskStatusPlus status = new TaskStatusPlus(
        "testId",
        "testType",
        DateTimes.nowUtc(),
        DateTimes.nowUtc(),
        TaskState.RUNNING,
        RunnerTaskState.RUNNING,
        1000L,
        TaskLocation.create("testHost", 1010, -1),
        "ds_test",
        null
    );
    final String json = mapper.writeValueAsString(status);
    Assert.assertEquals(status, mapper.readValue(json, TaskStatusPlus.class));
  }

  // Copied from org.apache.druid.jackson.JodaStuff
  private static class DateTimeDeserializer extends StdDeserializer<DateTime>
  {
    public DateTimeDeserializer()
    {
      super(DateTime.class);
    }

    @Override
    public DateTime deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException, JsonProcessingException
    {
      JsonToken t = jp.getCurrentToken();
      if (t == JsonToken.VALUE_NUMBER_INT) {
        return DateTimes.utc(jp.getLongValue());
      }
      if (t == JsonToken.VALUE_STRING) {
        String str = jp.getText().trim();
        if (str.length() == 0) { // [JACKSON-360]
          return null;
        }
        // make sure to preserve time zone information when parsing timestamps
        return DateTimes.ISO_DATE_OR_TIME_WITH_OFFSET.parse(str);
      }
      throw ctxt.mappingException(getValueClass());
    }
  }
}
