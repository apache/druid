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

package io.druid.query.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

@RunWith(Parameterized.class)
public class TimestampMinMaxAggregatorTest
{
  Injector injector;
  ObjectMapper mapper;

  private TimestampAggregatorFactory aggregatorFactory;
  private ColumnSelectorFactory selectorFactory;
  private TestObjectColumnSelector selector;

  private Timestamp[] values = {
      Timestamp.valueOf("2014-01-02 11:00:00"),
      Timestamp.valueOf("2014-01-02 01:00:00"),
      Timestamp.valueOf("2014-01-02 05:00:00"),
      Timestamp.valueOf("2014-01-02 12:00:00"),
      Timestamp.valueOf("2014-01-02 12:00:00"),
      Timestamp.valueOf("2014-01-02 13:00:00"),
      Timestamp.valueOf("2014-01-02 06:00:00"),
      Timestamp.valueOf("2014-01-02 17:00:00"),
      Timestamp.valueOf("2014-01-02 12:00:00"),
      Timestamp.valueOf("2014-01-02 02:00:00")
  };

  @Parameterized.Parameters(name="{index}: Test for {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return Iterables.transform(
        ImmutableList.of(
            ImmutableList.of("timeMin", TimestampMinAggregatorFactory.class, Long.MAX_VALUE, Timestamp.valueOf("2014-01-02 01:00:00")),
            ImmutableList.of("timeMax", TimestampMaxAggregatorFactory.class, Long.MIN_VALUE, Timestamp.valueOf("2014-01-02 17:00:00"))
        ),
        new Function<List<?>, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private String aggType;
  private Class<? extends TimestampAggregatorFactory> aggClass;
  private Long initValue;
  private Timestamp expected;

  public TimestampMinMaxAggregatorTest(String aggType, Class<? extends TimestampAggregatorFactory> aggClass, Long initValue, Timestamp expected)
  {
    this.aggType = aggType;
    this.aggClass = aggClass;
    this.expected = expected;
    this.initValue = initValue;
  }

  @Before
  public void setup() throws Exception
  {
    injector =  Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
              }
            },
            new TimestampMinMaxModule()
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);

    String json = "{\"type\":\"" + aggType + "\",\"name\":\"" + aggType + "\",\"fieldName\":\"test\"}";

    aggregatorFactory = mapper.readValue(json, aggClass);
    selector = new TestObjectColumnSelector(values);
    selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.makeObjectColumnSelector("test")).andReturn(selector);
    EasyMock.replay(selectorFactory);
  }

  @Test
  public void testAggregator()
  {
    TimestampAggregator aggregator = (TimestampAggregator) aggregatorFactory.factorize(selectorFactory);

    for (Timestamp value: values) {
      aggregate(selector, aggregator);
    }

    Assert.assertEquals(expected, new Timestamp(aggregator.getLong()));

    aggregator.reset();

    Assert.assertEquals(initValue, aggregator.get());
  }

  @Test
  public void testBufferAggregator()
  {
    TimestampBufferAggregator aggregator = (TimestampBufferAggregator) aggregatorFactory.factorizeBuffered(selectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Longs.BYTES]);
    aggregator.init(buffer, 0);

    for (Timestamp value: values) {
      aggregate(selector, aggregator, buffer, 0);
    }

    Assert.assertEquals(expected, new Timestamp(aggregator.getLong(buffer, 0)));

    aggregator.init(buffer, 0);

    Assert.assertEquals(initValue, aggregator.get(buffer, 0));
  }

  private void aggregate(TestObjectColumnSelector selector, TimestampAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  private void aggregate(TestObjectColumnSelector selector, TimestampBufferAggregator agg, ByteBuffer buf, int pos)
  {
    agg.aggregate(buf, pos);
    selector.increment();
  }
}
