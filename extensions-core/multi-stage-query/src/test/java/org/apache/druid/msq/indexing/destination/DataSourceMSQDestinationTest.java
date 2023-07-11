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

package org.apache.druid.msq.indexing.destination;


import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class DataSourceMSQDestinationTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DataSourceMSQDestination.class)
                  .withNonnullFields("dataSource", "segmentGranularity", "segmentSortOrder")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    DataSourceMSQDestination msqDestination = new DataSourceMSQDestination(
        "datasource",
        Granularities.DAY,
        null,
        null
    );

    final DataSourceMSQDestination msqDestination2 = mapper.readValue(
        mapper.writeValueAsString(msqDestination),
        DataSourceMSQDestination.class
    );


    Assert.assertEquals(msqDestination, msqDestination2);
  }
}
