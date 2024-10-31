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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.server.QueryStackTests;

public final class CachingClusteredClientTestUtils
{
  /**
   * Returns a new {@link QueryToolChestWarehouse} for unit tests and a resourceCloser which should be closed at the end
   * of the test.
   */
  public static Pair<QueryRunnerFactoryConglomerate, Closer> createWarehouse()
  {
    final Closer resourceCloser = Closer.create();
    QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
    return Pair.of(conglomerate, resourceCloser);
  }

  public static ObjectMapper createObjectMapper()
  {
    final SmileFactory factory = new SmileFactory();
    final ObjectMapper objectMapper = new DefaultObjectMapper(factory, "broker");
    factory.setCodec(objectMapper);
    return objectMapper;
  }

  private CachingClusteredClientTestUtils()
  {
  }
}
