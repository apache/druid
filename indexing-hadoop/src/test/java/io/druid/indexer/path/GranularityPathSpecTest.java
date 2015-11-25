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
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.FSSpideringIterator;
import io.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FSSpideringIterator.class, StaticPathSpec.class })
@SuppressStaticInitializationFor("io.druid.indexer.HadoopDruidIndexerConfig")
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

  @Test
  public void testAddInputPath() throws Exception
  {
    Iterable<FileStatus> testIterable = new Iterable<FileStatus>() {
      private int j;

      @Override
      public Iterator<FileStatus> iterator() {
        if (++j % 2 == 0) {
          throw new RuntimeException("Exception!", new FileNotFoundException());
        }

        return new Iterator<FileStatus>() {
          private int i;

          @Override
          public boolean hasNext() {
            return ++i <= 3;
          }

          @Override
          public FileStatus next() {
            return new FileStatus(0L, false, 0, 0L, 0L, new Path(String.format("file:///test/path/%d/%d.gz", j, i)));
          }
        };
      }
    };

    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("test", new String[]{"testGroup"}));
    granularityPathSpec.setDataGranularity(Granularity.HOUR);
    granularityPathSpec.setInputPath("/test/path");
    granularityPathSpec.setFilePattern("file:/+test/path/\\d+/\\d+.gz");
    granularityPathSpec.setInputFormat(CombineTextInputFormat.class);

    HadoopDruidIndexerConfig mockIndexerConfig = EasyMock.createNiceMock(HadoopDruidIndexerConfig.class);
    Capture<String> pathCapture = Capture.newInstance(CaptureType.ALL);
    Job job = Job.getInstance();

    PowerMock.mockStatic(FSSpideringIterator.class);
    PowerMock.mockStatic(StaticPathSpec.class);
    EasyMock.expect(FSSpideringIterator.spiderIterable(anyObject(FileSystem.class), anyObject(Path.class)))
        .andReturn(testIterable).times(3);

    StaticPathSpec.addToMultipleInputs(eq(mockIndexerConfig), eq(job),
        EasyMock.capture(pathCapture), eq(CombineTextInputFormat.class));
    PowerMock.expectLastCall().times(6);

    EasyMock.expect(mockIndexerConfig.getSegmentGranularIntervals())
        .andReturn(Optional.<Set<Interval>>of(Sets.newHashSet(Interval.parse("2015-11-10T00:00Z/2015-11-10T03:00Z"))));

    PowerMock.replay(FSSpideringIterator.class, StaticPathSpec.class, mockIndexerConfig);
    granularityPathSpec.addInputPaths(mockIndexerConfig, job);

    PowerMock.verify(FSSpideringIterator.class, StaticPathSpec.class, mockIndexerConfig);
    Assert.assertEquals(6, pathCapture.getValues().size());
    Assert.assertTrue("Didn't find all expected paths", pathCapture.getValues().containsAll(Lists.newArrayList(
        "file:/test/path/1/1.gz", "file:/test/path/1/2.gz", "file:/test/path/1/3.gz",
        "file:/test/path/3/1.gz", "file:/test/path/3/2.gz", "file:/test/path/3/3.gz"
    )));
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
