package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BrokerInternalQueryConfigTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    //defaults
    String json = "{}";

    BrokerInternalQueryConfig config = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(json, BrokerInternalQueryConfig.class)
        ),
        BrokerInternalQueryConfig.class
    );

    Assert.assertNull(config.getContext());

    //non-defaults
    json = "{ \"context\": {\"priority\": 5}}";

    config = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(json, BrokerInternalQueryConfig.class)
        ),
        BrokerInternalQueryConfig.class
    );

    Map<String, Object> expected = new HashMap<>();
    expected.put("priority", 5);
    Assert.assertEquals(expected, config.getContext());

  }
}
