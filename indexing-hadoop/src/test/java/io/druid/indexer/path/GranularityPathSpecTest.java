/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularities;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GranularityPathSpecTest
{
  private GranularityPathSpec granularityPathSpec;
  private final String TEST_STRING_PATH = "TEST";
  private final String TEST_STRING_PATTERN = "*.TEST";
  private final String TEST_STRING_FORMAT = "F_TEST";

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Rule
  public final TemporaryFolder testFolder = new TemporaryFolder();

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
    Assert.assertEquals(granularity, granularityPathSpec.getDataGranularity());
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

  @Test
  public void testAddInputPath() throws Exception
  {
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.MINUTE,
                ImmutableList.of(new Interval("2015-11-06T00:00Z/2015-11-07T00:00Z"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(null, null, null),
        new HadoopTuningConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            false,
            false,
            false,
            null,
            false,
            false,
            null,
            null,
            null,
            false,
            false
        )
    );

    granularityPathSpec.setDataGranularity(Granularity.HOUR);
    granularityPathSpec.setFilePattern(".*");
    granularityPathSpec.setInputFormat(TextInputFormat.class);

    Job job = Job.getInstance();
    String formatStr = "file:%s/%s;org.apache.hadoop.mapreduce.lib.input.TextInputFormat";

    testFolder.newFolder("test", "y=2015", "m=11", "d=06", "H=00");
    testFolder.newFolder("test", "y=2015", "m=11", "d=06", "H=02");
    testFolder.newFolder("test", "y=2015", "m=11", "d=06", "H=05");
    testFolder.newFile("test/y=2015/m=11/d=06/H=00/file1");
    testFolder.newFile("test/y=2015/m=11/d=06/H=02/file2");
    testFolder.newFile("test/y=2015/m=11/d=06/H=05/file3");
    testFolder.newFile("test/y=2015/m=11/d=06/H=05/file4");

    granularityPathSpec.setInputPath(testFolder.getRoot().getPath() + "/test");

    granularityPathSpec.addInputPaths(HadoopDruidIndexerConfig.fromSpec(spec), job);

    String actual = job.getConfiguration().get("mapreduce.input.multipleinputs.dir.formats");

    String expected = Joiner.on(",").join(Lists.newArrayList(
        String.format(formatStr, testFolder.getRoot(), "test/y=2015/m=11/d=06/H=00/file1"),
        String.format(formatStr, testFolder.getRoot(), "test/y=2015/m=11/d=06/H=02/file2"),
        String.format(formatStr, testFolder.getRoot(), "test/y=2015/m=11/d=06/H=05/file3"),
        String.format(formatStr, testFolder.getRoot(), "test/y=2015/m=11/d=06/H=05/file4")
    ));

    Assert.assertEquals("Did not find expected input paths", expected, actual);
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
    // Double-check Jackson's lower-case enum support
    sb.append(granularity.toString().toLowerCase());
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
