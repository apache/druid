/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexer.path;

import com.metamx.common.Granularity;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class GranularityPathSpecTest
{
  private GranularityPathSpec granularityPathSpec;
  private final String TEST_STRING_PATH = "TEST";
  private final String TEST_STRING_PATTERN = "*.TEST";
  private final String TEST_STRING_FORMAT = "F_TEST";

  @Before public void setUp()
  {
    granularityPathSpec = new GranularityPathSpec();
  }

  @After public void tearDown()
  {
    granularityPathSpec = null;
  }

  @Test public void testSetInputPath()
  {
    granularityPathSpec.setInputPath(TEST_STRING_PATH);
    Assert.assertEquals(TEST_STRING_PATH,granularityPathSpec.getInputPath());
  }

  @Test public void testSetFilePattern()
  {
    granularityPathSpec.setFilePattern(TEST_STRING_PATTERN);
    Assert.assertEquals(TEST_STRING_PATTERN,granularityPathSpec.getFilePattern());
  }

  @Test public void testSetPathFormat()
  {
    granularityPathSpec.setPathFormat(TEST_STRING_FORMAT);
    Assert.assertEquals(TEST_STRING_FORMAT,granularityPathSpec.getPathFormat());
  }

  @Test public void testSetDataGranularity()
  {
    Granularity granularity = Granularity.DAY;
    granularityPathSpec.setDataGranularity(granularity);
    Assert.assertEquals(granularity,granularityPathSpec.getDataGranularity());
  }
}
