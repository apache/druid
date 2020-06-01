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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * Utility class for adding {@link TimeDimTuple}s to a {@link com.google.common.hash.BloomFilter}.
 */
public enum TimeDimTupleFunnel implements Funnel<TimeDimTuple>
{
  INSTANCE;

  @Override
  public void funnel(TimeDimTuple timeDimTuple, PrimitiveSink into)
  {
    into.putLong(timeDimTuple.getTimestamp())
        .putUnencodedChars(timeDimTuple.getDimensionValue());
  }
}
