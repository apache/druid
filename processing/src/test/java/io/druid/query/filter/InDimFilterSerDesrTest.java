package io.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class InDimFilterSerDesrTest
{
  private static ObjectMapper mapper;

  private final String actualInFilter = "{\"type\":\"in\",\"dimension\":\"dimTest\",\"values\":[\"good\",\"bad\"]}";
  @Before
  public void setUp()
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  @Test
  public void testDeserialization() throws IOException
  {
    final InDimFilter actualInDimFilter = mapper.reader(DimFilter.class).readValue(actualInFilter);
    final InDimFilter expectedInDimFilter = new InDimFilter("dimTest", Arrays.asList("good", "bad"));
    Assert.assertEquals(expectedInDimFilter, actualInDimFilter);
  }

  @Test
  public void testSerialization() throws IOException
  {
    final InDimFilter dimInFilter = new InDimFilter("dimTest", Arrays.asList("good", "bad"));
    final String expectedInFilter = mapper.writeValueAsString(dimInFilter);
    Assert.assertEquals(expectedInFilter, actualInFilter);
  }

  @Test
  public void testGetCacheKey()
  {
    final InDimFilter inDimFilter_1 = new InDimFilter("dimTest", Arrays.asList("good", "bad"));
    final InDimFilter inDimFilter_2 = new InDimFilter("dimTest", Arrays.asList("good,bad"));
    Assert.assertNotEquals(inDimFilter_1.getCacheKey(), inDimFilter_2.getCacheKey());
  }
}
