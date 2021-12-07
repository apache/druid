package org.apache.druid.segment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.data.ComparableArray;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ComparableArraySerializationTest
{
  private ObjectMapper objectMapper;

  @Before
  public void setUp() throws Exception
  {
    objectMapper = new ObjectMapper();
  }

  @After
  public void tearDown() throws Exception
  {
    objectMapper = null;
  }

  @Test
  public void testSerialization() throws JsonProcessingException
  {
    ComparableArray array = new ComparableArray(new String[]{"a", "b", "c"});
    Assert.assertEquals("[\"a\",\"b\",\"c\"]", objectMapper.writeValueAsString(array));
  }
}
