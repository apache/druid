package io.druid.emitter.ambari.metrics;

import com.google.common.collect.Maps;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.jackson.DefaultObjectMapper;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.hadoop.metrics2.sink.timeline.TimelineMetric;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(JUnitParamsRunner.class)
public class WhiteListBasedDruidToTimelineEventConverterTest
{
  final private String prefix = "druid";
  final private WhiteListBasedDruidToTimelineEventConverter defaultWhiteListBasedDruidToTimelineEventConverter = new WhiteListBasedDruidToTimelineEventConverter(
      prefix,
      "druid",
      null,
      new DefaultObjectMapper()
  );
  private ServiceMetricEvent event;
  private final DateTime createdTime = new DateTime();
  private final String hostname = "testHost:8080";
  private final String serviceName = "historical";
  private final String defaultNamespace = prefix + "." + serviceName;

  @Before
  public void setUp()
  {
    event = EasyMock.createMock(ServiceMetricEvent.class);
    EasyMock.expect(event.getHost()).andReturn(hostname).anyTimes();
    EasyMock.expect(event.getService()).andReturn(serviceName).anyTimes();
    EasyMock.expect(event.getCreatedTime()).andReturn(createdTime).anyTimes();
    EasyMock.expect(event.getUserDims()).andReturn(Maps.<String, Object>newHashMap()).anyTimes();
    EasyMock.expect(event.getValue()).andReturn(10).anyTimes();
    EasyMock.expect(event.getFeed()).andReturn("metrics").anyTimes();
  }

  @Test
  @Parameters(
      {
          "query/time, true",
          "query/node/ttfb, true",
          "query/segmentAndCache/time, true",
          "query/intervalChunk/time, false",
          "query/time/balaba, true",
          "query/tim, false",
          "segment/added/bytes, true",
          "segment/count, true",
          "segment/size, true",
          "segment/cost/raw, false",
          "coordinator/TIER_1 /cost/raw, false",
          "segment/Kost/raw, false",
          ", false",
          "word, false",
          "coordinator, false",
          "server/, false",
          "ingest/persists/time, true",
          "jvm/mem/init, true",
          "jvm/gc/count, true"
      }
  )
  public void testDefaultIsInWhiteList(String key, boolean expectedValue)
  {
    EasyMock.expect(event.getMetric()).andReturn(key).anyTimes();
    EasyMock.replay(event);
    boolean isIn = defaultWhiteListBasedDruidToTimelineEventConverter.druidEventToTimelineMetric(event) != null;
    Assert.assertEquals(expectedValue, isIn);
  }

  @Test
  @Parameters
  public void testGetName(ServiceMetricEvent serviceMetricEvent, String expectedPath)
  {
    TimelineMetric metric = defaultWhiteListBasedDruidToTimelineEventConverter.druidEventToTimelineMetric(serviceMetricEvent);
    String path = null;
    if (metric != null) {
      path = metric.getMetricName();
    }
    Assert.assertEquals(expectedPath, path);
  }

  private Object[] parametersForTestGetName()
  {
    return new Object[]{
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("id", "dummy_id")
                                            .setDimension("status", "some_status")
                                            .setDimension("numDimensions", "1")
                                            .setDimension("segment", "dummy_segment")
                                            .build(createdTime, "query/segment/time/balabla/more", 10)
                .build(serviceName, hostname),
            defaultNamespace + ".query/segment/time/balabla/more"
        },
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("dataSource", "some_data_source")
                                            .setDimension("tier", "_default_tier")
                                            .build(createdTime, "segment/max", 10)
                .build(serviceName, hostname),
            null
        },
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("dataSource", "data-source")
                                            .setDimension("type", "groupBy")
                                            .setDimension("interval", "2013/2015")
                                            .setDimension("some_random_dim1", "random_dim_value1")
                                            .setDimension("some_random_dim2", "random_dim_value2")
                                            .setDimension("hasFilters", "no")
                                            .setDimension("duration", "P1D")
                                            .setDimension("remoteAddress", "194.0.90.2")
                                            .setDimension("id", "ID")
                                            .setDimension("context", "{context}")
                                            .build(createdTime, "query/time", 10)
                .build(serviceName, hostname),
            defaultNamespace + ".data-source.groupBy.query/time"
        },
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("dataSource", "data-source")
                                            .setDimension("type", "groupBy")
                                            .setDimension("some_random_dim1", "random_dim_value1")
                                            .build(createdTime, "ingest/persists/count", 10)
                .build(serviceName, hostname),
            defaultNamespace + ".data-source.ingest/persists/count"
        },
        new Object[]{
            new ServiceMetricEvent.Builder().setDimension("bufferPoolName", "BufferPool")
                                            .setDimension("type", "groupBy")
                                            .setDimension("some_random_dim1", "random_dim_value1")
                                            .build(createdTime, "jvm/bufferpool/capacity", 10)
                .build(serviceName, hostname),
            null
        }
    };
  }
}
