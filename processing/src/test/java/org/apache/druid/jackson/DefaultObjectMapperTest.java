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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielders;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 *
 */
public class DefaultObjectMapperTest
{
  ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testDateTime() throws Exception
  {
    final DateTime time = DateTimes.nowUtc();

    Assert.assertEquals(StringUtils.format("\"%s\"", time), mapper.writeValueAsString(time));
  }

  @Test
  public void testYielder() throws Exception
  {
    final Sequence<Object> sequence = Sequences.simple(
        Arrays.asList(
            "a",
            "b",
            null,
            DateTimes.utc(2L),
            5,
            DateTimeZone.UTC,
            "c"
        )
    );

    Assert.assertEquals(
        "[\"a\",\"b\",null,\"1970-01-01T00:00:00.002Z\",5,\"UTC\",\"c\"]",
        mapper.writeValueAsString(Yielders.each(sequence))
    );
  }
}
