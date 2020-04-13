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

package org.apache.druid.query;

import com.google.common.base.Function;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;

/**
*/
public class ConcatQueryRunner<T> implements QueryRunner<T>
{
  private final Sequence<QueryRunner<T>> queryRunners;

  public ConcatQueryRunner(Sequence<QueryRunner<T>> queryRunners)
  {
    this.queryRunners = queryRunners;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    return Sequences.concat(
        Sequences.map(
            queryRunners,
            new Function<QueryRunner<T>, Sequence<T>>()
            {
              @Override
              public Sequence<T> apply(final QueryRunner<T> input)
              {
                return input.run(queryPlus, responseContext);
              }
            }
        )
    );
  }
}
