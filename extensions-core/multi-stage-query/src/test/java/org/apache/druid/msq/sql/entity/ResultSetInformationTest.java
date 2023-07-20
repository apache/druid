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


package org.apache.druid.msq.sql.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.Assert;
import org.junit.Test;

public class ResultSetInformationTest
{
  public static final ObjectMapper MAPPER = new ObjectMapper();

  public static final ResultSetInformation RESULTS = new ResultSetInformation(
      1L,
      1L,
      ResultFormat.OBJECT,
      "ds",
      null,
      ImmutableList.of(new PageInformation(0, null, 1L))
  );


  public static final ResultSetInformation RESULTS_1 = new ResultSetInformation(
      1L,
      1L,
      ResultFormat.OBJECT,
      "ds",
      ImmutableList.of(
          new String[]{"1"},
          new String[]{"2"},
          new String[]{"3"}
      ),
      ImmutableList.of(new PageInformation(0, 1L, 1L))
  );
  public static final String JSON_STRING = "{\"numTotalRows\":1,\"totalSizeInBytes\":1,\"resultFormat\":\"object\",\"dataSource\":\"ds\",\"pages\":[{\"id\":0,\"sizeInBytes\":1}]}";
  public static final String JSON_STRING_1 = "{\"numTotalRows\":1,\"totalSizeInBytes\":1,\"resultFormat\":\"object\",\"dataSource\":\"ds\",\"sampleRecords\":[[\"1\"],[\"2\"],[\"3\"]],\"pages\":[{\"id\":0,\"numRows\":1,\"sizeInBytes\":1}]}";

  @Test
  public void sanityTest() throws JsonProcessingException
  {
    Assert.assertEquals(JSON_STRING, MAPPER.writeValueAsString(RESULTS));
    Assert.assertEquals(RESULTS, MAPPER.readValue(MAPPER.writeValueAsString(RESULTS), ResultSetInformation.class));
    Assert.assertEquals(
        RESULTS.hashCode(),
        MAPPER.readValue(MAPPER.writeValueAsString(RESULTS), ResultSetInformation.class).hashCode()
    );
    Assert.assertEquals(
        "ResultSetInformation{numTotalRows=1, totalSizeInBytes=1, resultFormat=object, records=null, dataSource='ds', pages=[PageInformation{id=0, numRows=null, sizeInBytes=1}]}",
        RESULTS.toString()
    );
  }

  @Test
  public void resultsSanityTest() throws JsonProcessingException
  {
    // Since we have a List<Object[]> as a field, we cannot call equals method after deserialization.
    Assert.assertEquals(JSON_STRING_1, MAPPER.writeValueAsString(RESULTS_1));
  }

}
