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

package org.apache.druid.storage.local;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.storage.StorageConfig;
import org.apache.druid.storage.StorageConnectorModule;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class LocalFileExportStorageProviderTest
{
  @Test
  public void testSerde() throws IOException
  {
    ExportStorageProvider exportDestination = new LocalFileExportStorageProvider("/basepath/export");

    ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.registerModules(new StorageConnectorModule().getJacksonModules());
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(StorageConfig.class, new StorageConfig("/"))
    );
    byte[] bytes = objectMapper.writeValueAsBytes(exportDestination);

    ExportStorageProvider deserialized = objectMapper.readValue(bytes, LocalFileExportStorageProvider.class);
    Assert.assertEquals(exportDestination, deserialized);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(LocalFileExportStorageProvider.class)
                  .withNonnullFields("exportPath")
                  .withIgnoredFields("storageConfig")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEmptyPath()
  {
    Assert.assertThrows(
        DruidException.class,
        () -> LocalFileExportStorageProvider.validateAndGetPath(null, "path")
    );
  }

  @Test
  public void testValidate()
  {
    File file = LocalFileExportStorageProvider.validateAndGetPath("/base", "/base/path");
    Assert.assertEquals("/base/path", file.toPath().toString());
  }

  @Test
  public void testWithNonSubdir()
  {
    Assert.assertThrows(
        DruidException.class,
        () -> LocalFileExportStorageProvider.validateAndGetPath("/base", "/base/../path")
    );
    Assert.assertThrows(
        DruidException.class,
        () -> LocalFileExportStorageProvider.validateAndGetPath("/base", "/base1")
    );
  }
}
