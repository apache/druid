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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class FilterTuningTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final FilterTuning filterTuning1 = new FilterTuning(false, 100, 200);
    Assert.assertEquals(
        filterTuning1,
        objectMapper.readValue(objectMapper.writeValueAsString(filterTuning1), FilterTuning.class)
    );

    final FilterTuning filterTuning2 = new FilterTuning(true, 100, 200);
    Assert.assertEquals(
        filterTuning2,
        objectMapper.readValue(objectMapper.writeValueAsString(filterTuning2), FilterTuning.class)
    );
  }
}
