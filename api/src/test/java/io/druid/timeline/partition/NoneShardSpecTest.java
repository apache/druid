package io.druid.timeline.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.TestObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class NoneShardSpecTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    final ShardSpec one = NoneShardSpec.instance();
    final ShardSpec two = NoneShardSpec.instance();
    Assert.assertEquals(one, two);
    Assert.assertEquals(one.hashCode(), two.hashCode());
  }

  @Test
  public void testSerde() throws Exception
  {
    final NoneShardSpec one = NoneShardSpec.instance();
    ObjectMapper mapper = new TestObjectMapper();
    NoneShardSpec serde1 = mapper.readValue(mapper.writeValueAsString(one), NoneShardSpec.class);
    NoneShardSpec serde2 = mapper.readValue(mapper.writeValueAsString(one), NoneShardSpec.class);

    // Serde should return same object instead of creating new one every time.
    Assert.assertTrue(serde1 == serde2);
    Assert.assertTrue(one == serde1);
  }
}
