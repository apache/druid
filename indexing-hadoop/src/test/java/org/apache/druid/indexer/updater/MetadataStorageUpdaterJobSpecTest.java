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

package org.apache.druid.indexer.updater;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class MetadataStorageUpdaterJobSpecTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Test
  public void testMetadaStorageConnectionConfigSimplePassword() throws Exception
  {
    testMetadataStorageUpdaterJobSpec(
        "segments_table",
        "db",
        "jdbc:mysql://localhost/druid",
        "druid",
        "\"nothing\"",
        "nothing"
    );
  }

  @Test
  public void testMetadaStorageConnectionConfigWithDefaultProviderPassword() throws Exception
  {
    testMetadataStorageUpdaterJobSpec(
        "segments_table",
        "db",
        "jdbc:mysql://localhost/druid",
        "druid",
        "{\"type\":\"default\",\"password\":\"nothing\"}",
        "nothing"
    );
  }

  private void testMetadataStorageUpdaterJobSpec(
      String segmentTable,
      String type,
      String connectURI,
      String user,
      String pwdString,
      String pwd
  ) throws Exception
  {
    MetadataStorageUpdaterJobSpec spec = JSON_MAPPER.readValue(
        "{" +
        "\"type\": \"" + type + "\",\n" +
        "\"connectURI\": \"" + connectURI + "\",\n" +
        "\"user\": \"" + user + "\",\n" +
        "\"password\": " + pwdString + ",\n" +
        "\"segmentTable\": \"" + segmentTable + "\"\n" +
        "}",
        MetadataStorageUpdaterJobSpec.class
    );

    Assert.assertEquals(segmentTable, spec.getSegmentTable());
    Assert.assertEquals(type, spec.getType());
    Assert.assertEquals("jdbc:mysql://localhost/druid", spec.get().getConnectURI());
    Assert.assertEquals(user, spec.get().getUser());
    Assert.assertEquals(pwd, spec.get().getPassword());
  }
}
