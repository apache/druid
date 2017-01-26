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

package io.druid.realtime.firehose;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.segment.realtime.firehose.ReplayableFirehoseFactory;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

public class ReplayableFirehoseFactoryTest extends EasyMockSupport
{
  private FirehoseFactory delegateFactory = createMock(FirehoseFactory.class);
  private Firehose delegateFirehose = createMock(Firehose.class);
  private InputRowParser parser = new MapInputRowParser(new TimeAndDimsParseSpec(null, null));
  private ObjectMapper mapper = new DefaultObjectMapper();


  private List<InputRow> testRows = Lists.<InputRow>newArrayList(
      new MapBasedInputRow(
          DateTime.now(), Lists.newArrayList("dim1", "dim2"),
          ImmutableMap.<String, Object>of("dim1", "val1", "dim2", "val2", "met1", 1)
      ),
      new MapBasedInputRow(
          DateTime.now(), Lists.newArrayList("dim1", "dim2"),
          ImmutableMap.<String, Object>of("dim1", "val5", "dim2", "val2", "met1", 2)
      ),
      new MapBasedInputRow(
          DateTime.now(), Lists.newArrayList("dim2", "dim3"),
          ImmutableMap.<String, Object>of("dim2", "val1", "dim3", "val2", "met1", 3)
      )
  );

  private ReplayableFirehoseFactory replayableFirehoseFactory;

  @Before
  public void setup()
  {
    replayableFirehoseFactory = new ReplayableFirehoseFactory(
        delegateFactory,
        true,
        10000,
        3,
        mapper
    );
  }

  @Test
  public void testConstructor() throws Exception
  {
    Assert.assertEquals(delegateFactory, replayableFirehoseFactory.getDelegateFactory());
    Assert.assertEquals(10000, replayableFirehoseFactory.getMaxTempFileSize());
    Assert.assertEquals(3, replayableFirehoseFactory.getReadFirehoseRetries());
    Assert.assertEquals(true, replayableFirehoseFactory.isReportParseExceptions());
  }

  @Test
  public void testReplayableFirehoseNoEvents() throws Exception
  {
    expect(delegateFactory.connect(parser)).andReturn(delegateFirehose);
    expect(delegateFirehose.hasMore()).andReturn(false);
    delegateFirehose.close();
    replayAll();

    try (Firehose firehose = replayableFirehoseFactory.connect(parser)) {
      Assert.assertFalse(firehose.hasMore());
    }
    verifyAll();
  }

  @Test
  public void testReplayableFirehoseWithEvents() throws Exception
  {
    final boolean hasMore[] = {true};

    expect(delegateFactory.connect(parser)).andReturn(delegateFirehose);
    expect(delegateFirehose.hasMore()).andAnswer(
        new IAnswer<Boolean>()
        {
          @Override
          public Boolean answer() throws Throwable
          {
            return hasMore[0];
          }
        }
    ).anyTimes();
    expect(delegateFirehose.nextRow())
        .andReturn(testRows.get(0))
        .andReturn(testRows.get(1))
        .andAnswer(
            new IAnswer<InputRow>()
            {
              @Override
              public InputRow answer() throws Throwable
              {
                hasMore[0] = false;
                return testRows.get(2);
              }
            }
        );

    delegateFirehose.close();
    replayAll();

    List<InputRow> rows = Lists.newArrayList();
    try (Firehose firehose = replayableFirehoseFactory.connect(parser)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
    Assert.assertEquals(testRows, rows);

    // now replay!
    rows.clear();
    try (Firehose firehose = replayableFirehoseFactory.connect(parser)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
    Assert.assertEquals(testRows, rows);

    verifyAll();
  }

  @Test
  public void testReplayableFirehoseWithoutReportParseExceptions() throws Exception
  {
    final boolean hasMore[] = {true};
    replayableFirehoseFactory = new ReplayableFirehoseFactory(
        delegateFactory,
        false,
        10000,
        3,
        mapper
    );

    expect(delegateFactory.connect(parser)).andReturn(delegateFirehose);
    expect(delegateFirehose.hasMore()).andAnswer(
        new IAnswer<Boolean>()
        {
          @Override
          public Boolean answer() throws Throwable
          {
            return hasMore[0];
          }
        }
    ).anyTimes();
    expect(delegateFirehose.nextRow())
        .andReturn(testRows.get(0))
        .andReturn(testRows.get(1))
        .andThrow(new ParseException("unparseable!"))
        .andAnswer(
            new IAnswer<InputRow>()
            {
              @Override
              public InputRow answer() throws Throwable
              {
                hasMore[0] = false;
                return testRows.get(2);
              }
            }
        );

    delegateFirehose.close();
    replayAll();

    List<InputRow> rows = Lists.newArrayList();
    try (Firehose firehose = replayableFirehoseFactory.connect(parser)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
    Assert.assertEquals(testRows, rows);

    verifyAll();
  }

  @Test(expected = ParseException.class)
  public void testReplayableFirehoseWithReportParseExceptions() throws Exception
  {
    final boolean hasMore[] = {true};

    expect(delegateFactory.connect(parser)).andReturn(delegateFirehose);
    expect(delegateFirehose.hasMore()).andAnswer(
        new IAnswer<Boolean>()
        {
          @Override
          public Boolean answer() throws Throwable
          {
            return hasMore[0];
          }
        }
    ).anyTimes();
    expect(delegateFirehose.nextRow())
        .andReturn(testRows.get(0))
        .andReturn(testRows.get(1))
        .andThrow(new ParseException("unparseable!"))
        .andAnswer(
            new IAnswer<InputRow>()
            {
              @Override
              public InputRow answer() throws Throwable
              {
                hasMore[0] = false;
                return testRows.get(2);
              }
            }
        );

    delegateFirehose.close();
    replayAll();

    replayableFirehoseFactory.connect(parser);
    verifyAll();
  }

  @Test
  public void testReplayableFirehoseWithConnectRetries() throws Exception
  {
    final boolean hasMore[] = {true};

    expect(delegateFactory.connect(parser)).andThrow(new IOException())
                                           .andReturn(delegateFirehose);
    expect(delegateFirehose.hasMore()).andAnswer(
        new IAnswer<Boolean>()
        {
          @Override
          public Boolean answer() throws Throwable
          {
            return hasMore[0];
          }
        }
    ).anyTimes();
    expect(delegateFirehose.nextRow())
        .andReturn(testRows.get(0))
        .andReturn(testRows.get(1))
        .andAnswer(
            new IAnswer<InputRow>()
            {
              @Override
              public InputRow answer() throws Throwable
              {
                hasMore[0] = false;
                return testRows.get(2);
              }
            }
        );
    delegateFirehose.close();
    replayAll();

    List<InputRow> rows = Lists.newArrayList();
    try (Firehose firehose = replayableFirehoseFactory.connect(parser)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
    Assert.assertEquals(testRows, rows);

    verifyAll();
  }

  @Test
  public void testReplayableFirehoseWithNextRowRetries() throws Exception
  {
    final boolean hasMore[] = {true};

    expect(delegateFactory.connect(parser)).andReturn(delegateFirehose).times(2);
    expect(delegateFirehose.hasMore()).andAnswer(
        new IAnswer<Boolean>()
        {
          @Override
          public Boolean answer() throws Throwable
          {
            return hasMore[0];
          }
        }
    ).anyTimes();
    expect(delegateFirehose.nextRow())
        .andReturn(testRows.get(0))
        .andThrow(new RuntimeException())
        .andReturn(testRows.get(0))
        .andReturn(testRows.get(1))
        .andAnswer(
            new IAnswer<InputRow>()
            {
              @Override
              public InputRow answer() throws Throwable
              {
                hasMore[0] = false;
                return testRows.get(2);
              }
            }
        );
    delegateFirehose.close();
    expectLastCall().times(2);
    replayAll();

    List<InputRow> rows = Lists.newArrayList();
    try (Firehose firehose = replayableFirehoseFactory.connect(parser)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
    Assert.assertEquals(testRows, rows);

    verifyAll();
  }

  @Test(expected = TestReadingException.class)
  public void testReplayableFirehoseWithNoRetries() throws Exception
  {
    replayableFirehoseFactory = new ReplayableFirehoseFactory(
        delegateFactory,
        false,
        10000,
        0,
        mapper
    );

    expect(delegateFactory.connect(parser)).andReturn(delegateFirehose);
    expect(delegateFirehose.hasMore()).andReturn(true).times(2);
    expect(delegateFirehose.nextRow()).andThrow(new TestReadingException());

    delegateFirehose.close();
    expectLastCall();
    replayAll();

    replayableFirehoseFactory.connect(parser);
    verifyAll();
  }

  @Test
  public void testReplayableFirehoseWithMultipleFiles() throws Exception
  {
    replayableFirehoseFactory = new ReplayableFirehoseFactory(delegateFactory, false, 1, 3, mapper);

    final boolean hasMore[] = {true};
    final int multiplicationFactor = 500;

    final InputRow finalRow = new MapBasedInputRow(
        DateTime.now(), Lists.newArrayList("dim4", "dim5"),
        ImmutableMap.<String, Object>of("dim4", "val12", "dim5", "val20", "met1", 30)
    );

    expect(delegateFactory.connect(parser)).andReturn(delegateFirehose);
    expect(delegateFirehose.hasMore()).andAnswer(
        new IAnswer<Boolean>()
        {
          @Override
          public Boolean answer() throws Throwable
          {
            return hasMore[0];
          }
        }
    ).anyTimes();

    expect(delegateFirehose.nextRow())
        .andReturn(testRows.get(0)).times(multiplicationFactor)
        .andReturn(testRows.get(1)).times(multiplicationFactor)
        .andReturn(testRows.get(2)).times(multiplicationFactor)
        .andAnswer(
            new IAnswer<InputRow>()
            {
              @Override
              public InputRow answer() throws Throwable
              {
                hasMore[0] = false;
                return finalRow;
              }
            }
        );

    delegateFirehose.close();
    replayAll();

    List<InputRow> testRowsMultiplied = Lists.newArrayList();
    for (InputRow row : testRows) {
      for (int i = 0; i < multiplicationFactor; i++) {
        testRowsMultiplied.add(row);
      }
    }
    testRowsMultiplied.add(finalRow);

    List<InputRow> rows = Lists.newArrayList();
    try (Firehose firehose = replayableFirehoseFactory.connect(parser)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
    Assert.assertEquals(testRowsMultiplied, rows);

    // now replay!
    rows.clear();
    try (Firehose firehose = replayableFirehoseFactory.connect(parser)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
    Assert.assertEquals(testRowsMultiplied, rows);

    verifyAll();
  }

  private class TestReadingException extends RuntimeException
  {
  }
}



