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

  public static final ResultSetInformation RESULTS = new ResultSetInformation(ResultFormat.OBJECT, 1L, 1L, "ds",
                                                                              ImmutableList.of(
                                                                                  ImmutableList.of("1"),
                                                                                  ImmutableList.of("2"),
                                                                                  ImmutableList.of("3")
                                                                              )
  );
  public static final String JSON_STRING = "{\"resultFormat\":\"object\",\"numRows\":1,\"sizeInBytes\":1,\"dataSource\":\"ds\",\"sampleRecords\":[[\"1\"],[\"2\"],[\"3\"]]}";


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
        "ResultSetInformation{totalRows=1, totalSize=1, resultFormat=object, records=[[1], [2], [3]], dataSource='ds'}",
        RESULTS.toString()
    );
  }
}
