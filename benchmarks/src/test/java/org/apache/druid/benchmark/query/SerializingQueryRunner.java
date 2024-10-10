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

package org.apache.druid.benchmark.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.ResultRow;

public class SerializingQueryRunner implements QueryRunner<ResultRow>
{
  static {
    NullHandling.initializeForTests();
  }

  private final ObjectMapper smileMapper;
  private final QueryRunner<ResultRow> baseRunner;
  private final Class<ResultRow> clazz;

  public SerializingQueryRunner(
      ObjectMapper smileMapper,
      Class<ResultRow> clazz,
      QueryRunner<ResultRow> baseRunner
  )
  {
    this.smileMapper = smileMapper;
    this.clazz = clazz;
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<ResultRow> run(
      final QueryPlus<ResultRow> queryPlus,
      final ResponseContext responseContext
  )
  {
    return Sequences.map(
        baseRunner.run(queryPlus, responseContext),
        input -> {
          try {
            return JacksonUtils.readValue(smileMapper, smileMapper.writeValueAsBytes(input.getArray()), clazz);
          }
          catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        }
    );
  }
}
