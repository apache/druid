/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class BucketExtractionFnTest
{
  private static final double DELTA = 0.0000001;

  @Test
  public void testApply()
  {
    BucketExtractionFn extractionFn1 = new BucketExtractionFn(100.0, 0.5);
    Assert.assertEquals("1200.5", extractionFn1.apply((Object) "1234.99"));
    Assert.assertEquals("1200.5", extractionFn1.apply("1234.99"));
    Assert.assertEquals("0.5", extractionFn1.apply("1"));
    Assert.assertEquals("0.5", extractionFn1.apply("100"));
    Assert.assertEquals("500.5", extractionFn1.apply(501));
    Assert.assertEquals("-399.5", extractionFn1.apply("-325"));
    Assert.assertEquals("2400.5", extractionFn1.apply("2.42e3"));
    Assert.assertEquals("-99.5", extractionFn1.apply("1.2e-1"));
    Assert.assertEquals(null, extractionFn1.apply("should be null"));
    Assert.assertEquals(null, extractionFn1.apply(""));

    BucketExtractionFn extractionFn2 = new BucketExtractionFn(3.0, 2.0);
    Assert.assertEquals("2", extractionFn2.apply("2"));
    Assert.assertEquals("2", extractionFn2.apply("3"));
    Assert.assertEquals("2", extractionFn2.apply("4.22"));
    Assert.assertEquals("-10", extractionFn2.apply("-8"));
    Assert.assertEquals("71", extractionFn2.apply("7.1e1"));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    BucketExtractionFn extractionFn1 = new BucketExtractionFn(100.0, 0.5);
    BucketExtractionFn extractionFn2 = new BucketExtractionFn(3.0, 2.0);
    BucketExtractionFn extractionFn3 = new BucketExtractionFn(3.0, 2.0);

    Assert.assertNotEquals(extractionFn1, extractionFn2);
    Assert.assertNotEquals(extractionFn1.hashCode(), extractionFn2.hashCode());
    Assert.assertEquals(extractionFn2, extractionFn3);
    Assert.assertEquals(extractionFn2.hashCode(), extractionFn3.hashCode());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final String json1 = "{ \"type\" : \"bucket\", \"size\" : \"2\", \"offset\" : \"0.5\" }";
    BucketExtractionFn extractionFn1 = (BucketExtractionFn) objectMapper.readValue(json1, ExtractionFn.class);
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
    BucketExtractionFn extractionFn2 = (BucketExtractionFn) objectMapper.readValue(json2, ExtractionFn.class);
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
