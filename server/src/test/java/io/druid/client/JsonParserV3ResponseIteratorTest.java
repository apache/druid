/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import io.druid.query.QueryInterruptedException;
import io.druid.segment.TestHelper;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class JsonParserV3ResponseIteratorTest
{
  @Test
  public void testSimpleResponse() throws Exception
  {
    String response = "{\n"
                      + "  \"result\": [\"v1\", \"v2\"],\n"
                      + "  \"context\": {\n"
                      + "    \"k\": \"v\"\n"
                      + "  }\n"
                      + "}";

    doTest(
        response,
        ImmutableList.of("v1", "v2"),
        ImmutableMap.<String, Object>of("host1", ImmutableMap.<String, Object>of("k", "v"))
    );
  }

  @Test
  public void testEmptyResponse() throws Exception
  {
    String response = "{\n"
                      + "  \"result\": [],\n"
                      + "  \"context\": { }\n"
                      + "}";

    doTest(response, Collections.<String>emptyList(), ImmutableMap.<String, Object>of());
  }

  @Test (expected = QueryInterruptedException.class)
  public void testErrorResponse() throws Exception
  {
    String response = "{\n"
                      + "  \"error\": \"Query timeout\"\n"
                      + "}";
    doTest(response, null, null);
  }

  private void doTest(String response, List<String> expectedValues, Map<String, Object> expectedContext)
      throws Exception
  {
    ObjectMapper objectMapper = TestHelper.JSON_MAPPER;

    final TypeFactory typeFactory = objectMapper.getTypeFactory();
    JavaType baseType = typeFactory.constructType(new TypeReference<String>(){});

    Future<InputStream> responseFuture = Futures.<InputStream>immediateFuture(
        IOUtils.toInputStream(response)
    );

    Map<String, Object> context = new HashMap<>();

    DirectDruidClient.JsonParserV3ResponseIterator<String> parser = new DirectDruidClient.JsonParserV3ResponseIterator<>(
        baseType,
        responseFuture,
        "http://dummy/",
        objectMapper,
        "host1",
        context
    );

    List<String> resultValues = new ArrayList<>();
    while (parser.hasNext()) {
      resultValues.add(parser.next());
    }

    Assert.assertEquals(resultValues, expectedValues);
    Assert.assertEquals(context, expectedContext);

    Assert.assertEquals(responseFuture.get().available(), 0);
  }
}
