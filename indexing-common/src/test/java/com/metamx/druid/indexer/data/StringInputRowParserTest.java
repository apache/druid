package com.metamx.druid.indexer.data;

import junit.framework.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class StringInputRowParserTest {



  @Test
  public void testPayloadSize() {
    ByteBuffer payload = ByteBuffer.allocate(10);
    payload.position(2);
    payload.limit(5);
    payload.rewind();
    Assert.assertEquals(5, payload.limit());
  }
}
