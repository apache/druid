package io.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class BucketExtractionFnTest
{
  private static final double DELTA = 0.0000001;

  @Test
  public void testApply()
  {
    BucketExtractionFn extractionFn1 = new BucketExtractionFn(100.0, 0.5);
    Assert.assertEquals("1200.5", extractionFn1.apply("1234.99"));
    Assert.assertEquals("0.5", extractionFn1.apply("1"));
    Assert.assertEquals("0.5", extractionFn1.apply("100"));
    Assert.assertEquals("500.5", extractionFn1.apply(501));

    BucketExtractionFn extractionFn2 = new BucketExtractionFn(3.0, 2.0);
    Assert.assertEquals("2", extractionFn2.apply("2"));
    Assert.assertEquals("2", extractionFn2.apply("3"));
    Assert.assertEquals("2", extractionFn2.apply("4.22"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final String json1 = "{ \"type\" : \"bucket\", \"size\" : \"2\", \"offset\" : \"0.5\" }";
    BucketExtractionFn extractionFn1 = (BucketExtractionFn)objectMapper.readValue(json1, ExtractionFn.class);
    Assert.assertEquals(2, extractionFn1.getSize(), DELTA);
    Assert.assertEquals(0.5, extractionFn1.getOffset(), DELTA);

    Assert.assertEquals(
        extractionFn1,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn1),
            ExtractionFn.class
        )
    );

    final String json2 = "{ \"type\" : \"bucket\"}";
    BucketExtractionFn extractionFn2 = (BucketExtractionFn)objectMapper.readValue(json2, ExtractionFn.class);
    Assert.assertEquals(1, extractionFn2.getSize(), DELTA);
    Assert.assertEquals(0, extractionFn2.getOffset(), DELTA);

    Assert.assertEquals(
        extractionFn2,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn2),
            ExtractionFn.class
        )
    );
  }

}
