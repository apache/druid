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

package org.apache.druid.msq.querykit;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.query.Query;

import java.util.Map;

/**
 * Delegates to other {@link QueryKit} implementations based on the class of the {@link Query}.
 */
@SuppressWarnings("rawtypes")
public class MultiQueryKit implements QueryKit<Query<?>>
{
  private final Map<Class<? extends Query>, QueryKit> toolKitMap;

  public MultiQueryKit(final Map<Class<? extends Query>, QueryKit> toolKitMap)
  {
    this.toolKitMap = Preconditions.checkNotNull(toolKitMap, "toolKitMap");
  }

  @Override
  public QueryDefinition makeQueryDefinition(
      String queryId,
      Query<?> query,
      QueryKit<Query<?>> toolKitForSubQueries,
      ShuffleSpecFactory resultShuffleSpecFactory,
      int maxWorkerCount,
      int minStageNumber
  )
  {
    final QueryKit specificToolKit = toolKitMap.get(query.getClass());

    if (specificToolKit != null) {
      //noinspection unchecked
      return specificToolKit.makeQueryDefinition(
          queryId,
          query,
          this,
          resultShuffleSpecFactory,
          maxWorkerCount,
          minStageNumber
      );
    } else {
      throw new ISE("Unsupported query class [%s]", query.getClass().getName());
    }
  }
}
