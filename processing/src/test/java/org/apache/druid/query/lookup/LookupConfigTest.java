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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LookupConfigTest
{

  ObjectMapper mapper = TestHelper.makeJsonMapper();
  @TempDir
  Path tempDir;

  @Test
  public void testSerDesr() throws IOException
  {
    LookupConfig lookupConfig = new LookupConfig(Files.createTempFile(tempDir, "junit", null).toFile().getAbsolutePath());
    Assertions.assertEquals(
        lookupConfig,
        mapper.readerFor(LookupConfig.class).readValue(mapper.writeValueAsString(lookupConfig))
    );
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String json = "{\n"
                  + "  \"enableLookupSyncOnStartup\": false,\n"
                  + "  \"snapshotWorkingDir\": \"/tmp\",\n"
                  + "  \"numLookupLoadingThreads\": 4,\n"
                  + "  \"coordinatorFetchRetries\": 4,\n"
                  + "  \"lookupStartRetries\": 4,\n"
                  + "  \"coordinatorRetryDelay\": 100 \n"
                  + "}\n";
    LookupConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(json, LookupConfig.class)
        ),
        LookupConfig.class
    );

    Assertions.assertEquals("/tmp", config.getSnapshotWorkingDir());
    Assertions.assertEquals(false, config.getEnableLookupSyncOnStartup());
    Assertions.assertEquals(4, config.getNumLookupLoadingThreads());
    Assertions.assertEquals(4, config.getCoordinatorFetchRetries());
    Assertions.assertEquals(4, config.getLookupStartRetries());
    Assertions.assertEquals(100, config.getCoordinatorRetryDelay());
  }
}
