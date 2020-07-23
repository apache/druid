package org.apache.druid.client.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Author: frank.chen021@outlook.com
 * Date: 2020/7/22 5:23 下午
 */
public class RedisClusterCacheTest
{
  @Test
  public void testConfig() throws JsonProcessingException
  {
    ObjectMapper mapper = new ObjectMapper();
    RedisCacheConfig fromJson = mapper.readValue("{\"expiration\":1000}", RedisCacheConfig.class);
    Assert.assertEquals(1, fromJson.getExpiration().getSeconds());

    fromJson = mapper.readValue("{\"expiration\":\"PT1H\"}", RedisCacheConfig.class);
    Assert.assertEquals(3600, fromJson.getExpiration().getSeconds());
  }

  @Test
  public void test()
  {
    RedisCacheConfig config = new RedisCacheConfig();

    //cluster
  }
}
