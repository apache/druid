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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class HadoopTuningConfigTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    HadoopTuningConfig expected = new HadoopTuningConfig(
        "/tmp/workingpath",
        "version",
        null,
        null,
        null,
        null,
        100,
        null,
        true,
        true,
        true,
        true,
        null,
        true,
        true,
        null,
        null,
        null,
        true,
        true,
        null,
        null,
        null,
        null
    );

    HadoopTuningConfig actual = jsonReadWriteRead(JSON_MAPPER.writeValueAsString(expected), HadoopTuningConfig.class);

    Assert.assertEquals("/tmp/workingpath", actual.getWorkingPath());
    Assert.assertEquals("version", actual.getVersion());
    Assert.assertNotNull(actual.getPartitionsSpec());
    Assert.assertEquals(ImmutableMap.<Long, List<HadoopyShardSpec>>of(), actual.getShardSpecs());
    Assert.assertEquals(new IndexSpec(), actual.getIndexSpec());
    Assert.assertEquals(new IndexSpec(), actual.getIndexSpecForIntermediatePersists());
    Assert.assertEquals(100, actual.getRowFlushBoundary());
    Assert.assertEquals(true, actual.isLeaveIntermediate());
    Assert.assertEquals(true, actual.isCleanupOnFailure());
    Assert.assertEquals(true, actual.isOverwriteFiles());
    Assert.assertEquals(true, actual.isIgnoreInvalidRows());
    Assert.assertEquals(ImmutableMap.<String, String>of(), actual.getJobProperties());
    Assert.assertEquals(true, actual.isCombineText());
    Assert.assertEquals(true, actual.getUseCombiner());
    Assert.assertEquals(0, actual.getNumBackgroundPersistThreads());
    Assert.assertEquals(true, actual.isForceExtendableShardSpecs());
    Assert.assertEquals(true, actual.isUseExplicitVersion());
  }

  public static <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsBytes(JSON_MAPPER.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
