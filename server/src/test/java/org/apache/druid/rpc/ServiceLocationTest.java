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

package org.apache.druid.rpc;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Test;

public class ServiceLocationTest
{
  @Test
  public void test_fromDruidServerMetadata_withPort()
  {
    DruidServerMetadata druidServerMetadata = new DruidServerMetadata(
        "name",
        "hostName:9092",
        null,
        1,
        ServerType.INDEXER_EXECUTOR,
        "tier1",
        2
    );

    Assert.assertEquals(
        new ServiceLocation("hostName", 9092, -1, ""),
        ServiceLocation.fromDruidServerMetadata(druidServerMetadata)
    );
  }

  @Test
  public void test_fromDruidServerMetadata_withTlsPort()
  {
    DruidServerMetadata druidServerMetadata = new DruidServerMetadata(
        "name",
        null,
        "hostName:8100",
        1,
        ServerType.INDEXER_EXECUTOR,
        "tier1",
        2
    );

    Assert.assertEquals(
        new ServiceLocation("hostName", -1, 8100, ""),
        ServiceLocation.fromDruidServerMetadata(druidServerMetadata)
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ServiceLocation.class)
                  .usingGetClass()
                  .verify();
  }
}
