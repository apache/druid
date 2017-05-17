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

package io.druid.benchmark.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;

import java.util.Map;

public class SerializingQueryRunner<T> implements QueryRunner<T>
{
  private final ObjectMapper smileMapper;
  private final QueryRunner<T> baseRunner;
  private final Class<T> clazz;

  public SerializingQueryRunner(
      ObjectMapper smileMapper,
      Class<T> clazz,
      QueryRunner<T> baseRunner
  )
  {
    this.smileMapper = smileMapper;
    this.clazz = clazz;
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(
      final QueryPlus<T> queryPlus,
      final Map<String, Object> responseContext
  )
  {
    return Sequences.map(
        baseRunner.run(queryPlus, responseContext),
        new Function<T, T>()
        {
          @Override
          public T apply(T input)
          {
            try {
              return smileMapper.readValue(smileMapper.writeValueAsBytes(input), clazz);
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
    );
  }
}
