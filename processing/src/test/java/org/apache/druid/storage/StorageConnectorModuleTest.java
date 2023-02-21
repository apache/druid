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

package org.apache.druid.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.druid.storage.local.LocalFileStorageConnector;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class StorageConnectorModuleTest
{
  private static final String JSON = "{\n"
                                     + "        \"type\": \"local\",\n"
                                     + "        \"basePath\": \"/tmp\"\n"
                                     + "}";

  private static final String JSON_WITHOUT_PATH = "{\n"
                                                  + "        \"type\": \"local\"\n"
                                                  + "}";

  final ObjectMapper objectMapper = new ObjectMapper()
      .registerModules(new StorageConnectorModule().getJacksonModules());

  @Test
  public void testJsonSerde() throws JsonProcessingException
  {
    StorageConnectorProvider storageConnectorProvider = objectMapper.readValue(JSON, StorageConnectorProvider.class);
    Assert.assertTrue(storageConnectorProvider.get() instanceof LocalFileStorageConnector);
    Assert.assertEquals(new File("/tmp"), ((LocalFileStorageConnector) storageConnectorProvider.get()).getBasePath());
  }


  @Test
  public void testJsonSerdeWithoutPath()
  {
    Assert.assertThrows(
        "Missing required creator property 'basePath'",
        MismatchedInputException.class,
        () -> objectMapper.readValue(
            JSON_WITHOUT_PATH,
            StorageConnectorProvider.class
        )
    );
  }
}
