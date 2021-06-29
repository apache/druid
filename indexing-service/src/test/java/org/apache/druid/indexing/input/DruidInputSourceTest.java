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

package org.apache.druid.indexing.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.guice.IndexingServiceInputSourceModule;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DruidInputSourceTest
{
  private final IndexIO indexIO = EasyMock.createMock(IndexIO.class);
  private final CoordinatorClient coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
  private final SegmentLoaderFactory segmentLoaderFactory = EasyMock.createMock(SegmentLoaderFactory.class);
  private final RetryPolicyFactory retryPolicyFactory = EasyMock.createMock(RetryPolicyFactory.class);
  private final TaskConfig taskConfig = EasyMock.createMock(TaskConfig.class);

  private ObjectMapper mapper = null;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp()
  {
    mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(new IndexingServiceInputSourceModule().getJacksonModules());

    final InjectableValues.Std injectableValues = (InjectableValues.Std) mapper.getInjectableValues();
    injectableValues.addValue(IndexIO.class, indexIO);
    injectableValues.addValue(CoordinatorClient.class, coordinatorClient);
    injectableValues.addValue(SegmentLoaderFactory.class, segmentLoaderFactory);
    injectableValues.addValue(RetryPolicyFactory.class, retryPolicyFactory);
    injectableValues.addValue(TaskConfig.class, taskConfig);
  }

  @Test
  public void testSerdeUsingIntervals() throws Exception
  {
    final String json = "{"
                        + "\"type\":\"druid\","
                        + "\"dataSource\":\"foo\","
                        + "\"interval\":\"2000-01-01T00:00:00.000Z/2001-01-01T00:00:00.000Z\""
                        + "}";

    final InputSource inputSource = mapper.readValue(json, InputSource.class);

    Assert.assertThat(inputSource, CoreMatchers.instanceOf(DruidInputSource.class));
    Assert.assertEquals(
        new DruidInputSource(
            "foo",
            Intervals.of("2000/2001"),
            null,
            null,
            null,
            null,
            indexIO,
            coordinatorClient,
            segmentLoaderFactory,
            retryPolicyFactory,
            taskConfig
        ),
        inputSource
    );

    Assert.assertEquals(json, mapper.writeValueAsString(inputSource));
  }

  @Test
  public void testSerdeUsingIntervalsAndLegacyDimensionsMetrics() throws Exception
  {
    final String json = "{"
                        + "\"type\":\"druid\","
                        + "\"dataSource\":\"foo\","
                        + "\"interval\":\"2000-01-01T00:00:00.000Z/2001-01-01T00:00:00.000Z\","
                        + "\"dimensions\":[\"a\"],"
                        + "\"metrics\":[\"b\"]"
                        + "}";

    final InputSource inputSource = mapper.readValue(json, InputSource.class);

    Assert.assertThat(inputSource, CoreMatchers.instanceOf(DruidInputSource.class));
    Assert.assertEquals(
        new DruidInputSource(
            "foo",
            Intervals.of("2000/2001"),
            null,
            null,
            ImmutableList.of("a"),
            ImmutableList.of("b"),
            indexIO,
            coordinatorClient,
            segmentLoaderFactory,
            retryPolicyFactory,
            taskConfig
        ),
        inputSource
    );

    Assert.assertEquals(json, mapper.writeValueAsString(inputSource));
  }

  @Test
  public void testSerdeUsingSegments() throws Exception
  {
    final String json = "{"
                        + "\"type\":\"druid\","
                        + "\"dataSource\":\"foo\","
                        + "\"segments\":["
                        + "{\"segmentId\":\"foo_2000-01-01T00:00:00.000Z_2000-01-01T01:00:00.000Z_abc123\","
                        + "\"intervals\":[\"2000-01-01T00:00:00.000Z/2000-01-01T12:00:00.000Z\"]}"
                        + "]"
                        + "}";

    final InputSource inputSource = mapper.readValue(json, InputSource.class);

    Assert.assertThat(inputSource, CoreMatchers.instanceOf(DruidInputSource.class));
    Assert.assertEquals(
        new DruidInputSource(
            "foo",
            null,
            ImmutableList.of(
                new WindowedSegmentId(
                    "foo_2000-01-01T00:00:00.000Z_2000-01-01T01:00:00.000Z_abc123",
                    ImmutableList.of(Intervals.of("2000-01-01T00/2000-01-01T12"))
                )
            ),
            null,
            null,
            null,
            indexIO,
            coordinatorClient,
            segmentLoaderFactory,
            retryPolicyFactory,
            taskConfig
        ),
        inputSource
    );

    Assert.assertEquals(json, mapper.writeValueAsString(inputSource));
  }

  @Test
  public void testSerdeUsingBothIntervalsAndSegments() throws Exception
  {
    final String json = "{"
                        + "\"type\":\"druid\","
                        + "\"dataSource\":\"foo\","
                        + "\"interval\":\"2000-01-01T00:00:00.000Z/2001-01-01T00:00:00.000Z\","
                        + "\"segments\":["
                        + "  {\"segmentId\":\"foo_2000-01-01T00:00:00.000Z_2000-01-01T01:00:00.000Z_abc123\","
                        + "   \"intervals\":[\"2000-01-01T00:00:00.000Z/2000-01-01T12:00:00.000Z\"]}"
                        + "]"
                        + "}";


    expectedException.expect(JsonProcessingException.class);
    expectedException.expectMessage("Specify exactly one of 'interval' and 'segments'");

    mapper.readValue(json, InputSource.class);
  }

  @Test
  public void testSerdeUsingNeitherIntervalsNorSegments() throws Exception
  {
    final String json = "{"
                        + "\"type\":\"druid\","
                        + "\"dataSource\":\"foo\""
                        + "}";

    expectedException.expect(JsonProcessingException.class);
    expectedException.expectMessage("Specify exactly one of 'interval' and 'segments'");

    mapper.readValue(json, InputSource.class);
  }

  @Test
  public void testSerdeUsingNoDataSource() throws Exception
  {
    final String json = "{"
                        + "\"type\":\"druid\","
                        + "\"interval\":\"2000-01-01T00:00:00.000Z/2001-01-01T00:00:00.000Z\""
                        + "}";

    expectedException.expect(JsonProcessingException.class);
    expectedException.expectMessage("dataSource");

    mapper.readValue(json, InputSource.class);
  }
}
