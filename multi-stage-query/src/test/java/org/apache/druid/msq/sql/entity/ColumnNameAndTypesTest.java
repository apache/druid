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
import org.junit.Assert;
import org.junit.Test;

public class ColumnNameAndTypesTest
{
  public static final ObjectMapper MAPPER = new ObjectMapper();

  public static final ColumnNameAndTypes COLUMN_NAME_AND_TYPES = new ColumnNameAndTypes("test", "test1", "test2");
  public static final String JSON_STRING = "{\"name\":\"test\",\"type\":\"test1\",\"nativeType\":\"test2\"}";

  @Test
  public void sanityTest() throws JsonProcessingException
  {
    Assert.assertEquals(JSON_STRING, MAPPER.writeValueAsString(COLUMN_NAME_AND_TYPES));
    Assert.assertEquals(
        COLUMN_NAME_AND_TYPES,
        MAPPER.readValue(MAPPER.writeValueAsString(COLUMN_NAME_AND_TYPES), ColumnNameAndTypes.class)
    );

    Assert.assertEquals(
        COLUMN_NAME_AND_TYPES.hashCode(),
        MAPPER.readValue(MAPPER.writeValueAsString(COLUMN_NAME_AND_TYPES), ColumnNameAndTypes.class)
              .hashCode()
    );
    Assert.assertEquals("ColumnNameAndTypes{colName='test', sqlTypeName='test1', nativeTypeName='test2'}",
                        COLUMN_NAME_AND_TYPES.toString());

  }

}
