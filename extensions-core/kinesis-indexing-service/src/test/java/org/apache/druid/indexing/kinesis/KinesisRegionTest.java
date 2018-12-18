package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class KinesisRegionTest
{
  private ObjectMapper mapper;

  @Before
  public void setupTest()
  {
    mapper = new DefaultObjectMapper();
  }

  @Test
  public void testSerde() throws IOException
  {
    KinesisRegion kinesisRegionUs1 = KinesisRegion.US_EAST_1;
    KinesisRegion kinesisRegionAp1 = KinesisRegion.AP_NORTHEAST_1;

    Assert.assertEquals("\"us-east-1\"", mapper.writeValueAsString(kinesisRegionUs1));
    Assert.assertEquals("\"ap-northeast-1\"", mapper.writeValueAsString(kinesisRegionAp1));

    KinesisRegion kinesisRegion = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                "\"us-east-1\"",
                KinesisRegion.class
            )
        ),
        KinesisRegion.class
    );

    Assert.assertEquals(kinesisRegion, KinesisRegion.US_EAST_1);
  }

  @Test(expected = JsonMappingException.class)
  public void testBadSerde() throws IOException
  {
    mapper.readValue(
        "\"us-east-10\"",
        KinesisRegion.class
    );
  }

}

