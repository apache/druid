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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.Granularity;
import io.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

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

  @Test
  public void testSerdeCustomInputFormat() throws Exception
  {
    testSerde("/test/path", "*.test", "pat_pat", Granularity.SECOND, TextInputFormat.class);
  }

  @Test
  public void testSerdeNoInputFormat() throws Exception
  {
    testSerde("/test/path", "*.test", "pat_pat", Granularity.SECOND, null);
  }

  private void testSerde(
      String inputPath,
      String filePattern,
      String pathFormat,
      Granularity granularity,
      Class inputFormat) throws Exception
  {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"inputPath\" : \"");
    sb.append(inputPath);
    sb.append("\",");
    sb.append("\"filePattern\" : \"");
    sb.append(filePattern);
    sb.append("\",");
    sb.append("\"pathFormat\" : \"");
    sb.append(pathFormat);
    sb.append("\",");
    sb.append("\"dataGranularity\" : \"");
    sb.append(granularity.toString());
    sb.append("\",");
    if(inputFormat != null) {
      sb.append("\"inputFormat\" : \"");
      sb.append(inputFormat.getName());
      sb.append("\",");
    }
    sb.append("\"type\" : \"granularity\"}");

    GranularityPathSpec pathSpec = (GranularityPathSpec) StaticPathSpecTest.readWriteRead(sb.toString(), jsonMapper);
    Assert.assertEquals(inputFormat, pathSpec.getInputFormat());
    Assert.assertEquals(inputPath, pathSpec.getInputPath());
    Assert.assertEquals(filePattern, pathSpec.getFilePattern());
    Assert.assertEquals(pathFormat, pathSpec.getPathFormat());
    Assert.assertEquals(granularity, pathSpec.getDataGranularity());
  }
}
