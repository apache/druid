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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;

@ExtensionPoint
public interface QueryRunner<T>
{
  /**
   * Runs the given query and returns results in a time-ordered sequence.
   */
  Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext);
  
  @VisibleForTesting
  default Sequence<T> run(QueryPlus<T> queryPlus)
  {
    return this.run(queryPlus, ResponseContext.createEmpty());
  }
}
