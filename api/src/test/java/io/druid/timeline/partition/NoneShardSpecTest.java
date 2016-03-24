package io.druid.timeline.partition;

import org.junit.Assert;
import org.junit.Test;

public class NoneShardSpecTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    final ShardSpec one = new NoneShardSpec();
    final ShardSpec two = new NoneShardSpec();
    Assert.assertEquals(one, two);
    Assert.assertEquals(one.hashCode(), two.hashCode());
  }
}
