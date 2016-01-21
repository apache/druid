package io.druid.emitter.graphite;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class GraphiteEmitterConfigTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
                ObjectMapper.class,
                new DefaultObjectMapper()
            ));
  }

  @Test
  public void testSerDeserGraphiteEmitterConfig() throws IOException
  {
    GraphiteEmitterConfig graphiteEmitterConfig = new GraphiteEmitterConfig(
        "hostname",
        8080,
        1000,
        1000L,
        100,
        new SendAllGraphiteEventConverter("prefix", true, true),
        Collections.EMPTY_LIST
    );
    String graphiteEmitterConfigString = mapper.writeValueAsString(graphiteEmitterConfig);
    GraphiteEmitterConfig graphiteEmitterConfigExpected = mapper.reader(GraphiteEmitterConfig.class).readValue(
        graphiteEmitterConfigString
    );
    Assert.assertEquals(graphiteEmitterConfigExpected, graphiteEmitterConfig);
  }

  @Test
  public void testSerDeserDruidToGraphiteEventConverter() throws IOException
  {
    SendAllGraphiteEventConverter sendAllGraphiteEventConverter = new SendAllGraphiteEventConverter("prefix", true, true);
    String noopGraphiteEventConverterString = mapper.writeValueAsString(sendAllGraphiteEventConverter);
    DruidToGraphiteEventConverter druidToGraphiteEventConverter = mapper.reader(DruidToGraphiteEventConverter.class)
                                                                        .readValue(noopGraphiteEventConverterString);
    Assert.assertEquals(druidToGraphiteEventConverter, sendAllGraphiteEventConverter);

    WhiteListBasedConverter whiteListBasedConverter = new WhiteListBasedConverter(
        "prefix",
        true,
        true,
        "",
        new DefaultObjectMapper()
    );
    String whiteListBasedConverterString = mapper.writeValueAsString(whiteListBasedConverter);
    druidToGraphiteEventConverter = mapper.reader(DruidToGraphiteEventConverter.class)
                                          .readValue(whiteListBasedConverterString);
    Assert.assertEquals(druidToGraphiteEventConverter, whiteListBasedConverter);
  }
}
