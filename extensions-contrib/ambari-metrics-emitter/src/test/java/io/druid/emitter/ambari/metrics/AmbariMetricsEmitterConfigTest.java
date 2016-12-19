package io.druid.emitter.ambari.metrics;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class AmbariMetricsEmitterConfigTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
                ObjectMapper.class,
                new DefaultObjectMapper()
            ));
  }

  @Test
  public void testSerDeAmbariMetricsEmitterConfig() throws IOException
  {
    AmbariMetricsEmitterConfig config = new AmbariMetricsEmitterConfig(
        "hostname",
        8080,
        "http",
        "truststore.path",
        "truststore.type",
        "truststore.password",
        1000,
        1000L,
        100,
        new SendAllTimelineEventConverter("prefix", "druid"),
        Collections.EMPTY_LIST,
        500L,
        400L
    );
    AmbariMetricsEmitterConfig serde = mapper.reader(AmbariMetricsEmitterConfig.class).readValue(
        mapper.writeValueAsBytes(config)
    );
    Assert.assertEquals(config, serde);
  }

  @Test
  public void testSerDeDruidToTimelineEventConverter() throws IOException
  {
    SendAllTimelineEventConverter sendAllConverter = new SendAllTimelineEventConverter("prefix", "druid");
    DruidToTimelineMetricConverter serde = mapper.reader(DruidToTimelineMetricConverter.class)
                                                                        .readValue( mapper.writeValueAsBytes(sendAllConverter));
    Assert.assertEquals(sendAllConverter, serde);

    WhiteListBasedDruidToTimelineEventConverter whiteListBasedDruidToTimelineEventConverter = new WhiteListBasedDruidToTimelineEventConverter(
        "prefix",
        "druid",
        "",
        new DefaultObjectMapper()
    );
    serde = mapper.reader(DruidToTimelineMetricConverter.class)
                                          .readValue(mapper.writeValueAsBytes(
                                              whiteListBasedDruidToTimelineEventConverter));
    Assert.assertEquals(whiteListBasedDruidToTimelineEventConverter, serde);
  }
}
