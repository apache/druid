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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;

import java.util.List;
import java.util.Map;

public class TestQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(TestQueryRunner.class);

  private final Sequence<T> sequence;

  public TestQueryRunner(List<T> iterable)
  {
    sequence = Sequences.simple(iterable);
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, Map<String, Object> responseContext)
  {
    return sequence;
  }
}
