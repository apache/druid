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

package org.apache.druid.msq.querykit.results;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.storage.StorageConfig;
import org.apache.druid.storage.StorageConnectorModule;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ExportResultsStageProcessorTest
{
  @Test
  public void testSerde() throws IOException
  {
    String exportFactoryString = "{\"type\":\"exportResults\",\"queryId\":\"query-9128ieio9wq\",\"exportStorageProvider\":{\"type\":\"local\",\"exportPath\":\"/path\"},\"exportFormat\":\"csv\",\"resultTypeReference\":{\"type\":\"java.lang.Object\"}}";

    ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.registerModules(new StorageConnectorModule().getJacksonModules());
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(StorageConfig.class, new StorageConfig("/"))
    );

    ExportResultsStageProcessor exportResultsFrameProcessor = objectMapper.readValue(
        exportFactoryString,
        ExportResultsStageProcessor.class
    );
    Assert.assertNull(exportResultsFrameProcessor.getColumnMappings());
    Assert.assertNull(exportResultsFrameProcessor.getResultsContext());
  }
}
