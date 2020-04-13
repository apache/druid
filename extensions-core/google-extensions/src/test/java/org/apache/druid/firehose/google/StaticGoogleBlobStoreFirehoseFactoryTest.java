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

package org.apache.druid.firehose.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.google.GoogleCloudStorageInputSourceTest;
import org.apache.druid.storage.google.GoogleStorage;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class StaticGoogleBlobStoreFirehoseFactoryTest
{
  private static final GoogleStorage STORAGE = new GoogleStorage(null);

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = GoogleCloudStorageInputSourceTest.createGoogleObjectMapper();

    final List<GoogleBlob> blobs = ImmutableList.of(
        new GoogleBlob("foo", "bar"),
        new GoogleBlob("foo", "bar2")
    );

    final StaticGoogleBlobStoreFirehoseFactory factory = new StaticGoogleBlobStoreFirehoseFactory(
        STORAGE,
        blobs,
        2048L,
        1024L,
        512L,
        100L,
        5
    );

    final StaticGoogleBlobStoreFirehoseFactory outputFact = mapper.readValue(
        mapper.writeValueAsString(factory),
        StaticGoogleBlobStoreFirehoseFactory.class
    );

    Assert.assertEquals(factory, outputFact);
  }
}
