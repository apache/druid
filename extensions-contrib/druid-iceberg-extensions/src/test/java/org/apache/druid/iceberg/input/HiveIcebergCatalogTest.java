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

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

public class HiveIcebergCatalogTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testCatalogCreate()
  {
    final File warehouseDir = FileUtils.createTempDir();
    HiveIcebergCatalog hiveCatalog = new HiveIcebergCatalog(
        warehouseDir.getPath(),
        "hdfs://testuri",
        new HashMap<>(),
        mapper,
        new Configuration()
    );
    HiveIcebergCatalog hiveCatalogNullProps = new HiveIcebergCatalog(
        warehouseDir.getPath(),
        "hdfs://testuri",
        null,
        mapper,
        new Configuration()
    );
    Assert.assertEquals("hive", hiveCatalog.retrieveCatalog().name());
    Assert.assertEquals(2, hiveCatalogNullProps.getCatalogProperties().size());
  }

  @Test
  public void testAuthenticate()
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    final File warehouseDir = FileUtils.createTempDir();
    HashMap<String, Object> catalogMap = new HashMap<>();
    catalogMap.put("principal", "test");
    catalogMap.put("keytab", "test");
    HiveIcebergCatalog hiveCatalog = new HiveIcebergCatalog(
        warehouseDir.getPath(),
        "hdfs://testuri",
        catalogMap,
        mapper,
        new Configuration()
    );
    Assert.assertEquals("hdfs://testuri", hiveCatalog.getCatalogUri());

  }
}
