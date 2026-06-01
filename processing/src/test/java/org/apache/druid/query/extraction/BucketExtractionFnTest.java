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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BucketExtractionFnTest
{
  private static final double DELTA = 0.0000001;

  @Test
  public void testApply()
  {
    BucketExtractionFn extractionFn1 = new BucketExtractionFn(100.0, 0.5);
    Assertions.assertEquals("1200.5", extractionFn1.apply((Object) "1234.99"));
    Assertions.assertEquals("1200.5", extractionFn1.apply("1234.99"));
    Assertions.assertEquals("0.5", extractionFn1.apply("1"));
    Assertions.assertEquals("0.5", extractionFn1.apply("100"));
    Assertions.assertEquals("500.5", extractionFn1.apply(501));
    Assertions.assertEquals("-399.5", extractionFn1.apply("-325"));
    Assertions.assertEquals("2400.5", extractionFn1.apply("2.42e3"));
    Assertions.assertEquals("-99.5", extractionFn1.apply("1.2e-1"));
    Assertions.assertEquals(null, extractionFn1.apply("should be null"));
    Assertions.assertEquals(null, extractionFn1.apply(""));

    BucketExtractionFn extractionFn2 = new BucketExtractionFn(3.0, 2.0);
    Assertions.assertEquals("2", extractionFn2.apply("2"));
    Assertions.assertEquals("2", extractionFn2.apply("3"));
    Assertions.assertEquals("2", extractionFn2.apply("4.22"));
    Assertions.assertEquals("-10", extractionFn2.apply("-8"));
    Assertions.assertEquals("71", extractionFn2.apply("7.1e1"));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    BucketExtractionFn extractionFn1 = new BucketExtractionFn(100.0, 0.5);
    BucketExtractionFn extractionFn2 = new BucketExtractionFn(3.0, 2.0);
    BucketExtractionFn extractionFn3 = new BucketExtractionFn(3.0, 2.0);

    Assertions.assertNotEquals(extractionFn1, extractionFn2);
    Assertions.assertNotEquals(extractionFn1.hashCode(), extractionFn2.hashCode());
    Assertions.assertEquals(extractionFn2, extractionFn3);
    Assertions.assertEquals(extractionFn2.hashCode(), extractionFn3.hashCode());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final String json1 = "{ \"type\" : \"bucket\", \"size\" : \"2\", \"offset\" : \"0.5\" }";
    BucketExtractionFn extractionFn1 = (BucketExtractionFn) objectMapper.readValue(json1, ExtractionFn.class);
    Assertions.assertEquals(2, extractionFn1.getSize(), DELTA);
    Assertions.assertEquals(0.5, extractionFn1.getOffset(), DELTA);

    Assertions.assertEquals(
        extractionFn1,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn1),
            ExtractionFn.class
        )
    );

    final String json2 = "{ \"type\" : \"bucket\"}";
    BucketExtractionFn extractionFn2 = (BucketExtractionFn) objectMapper.readValue(json2, ExtractionFn.class);
    Assertions.assertEquals(1, extractionFn2.getSize(), DELTA);
    Assertions.assertEquals(0, extractionFn2.getOffset(), DELTA);

    Assertions.assertEquals(
        extractionFn2,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn2),
            ExtractionFn.class
        )
    );
  }

}
