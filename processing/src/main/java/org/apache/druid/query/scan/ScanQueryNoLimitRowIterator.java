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

package org.apache.druid.query.scan;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;

import java.io.IOException;
import java.util.Map;

public class ScanQueryNoLimitRowIterator implements CloseableIterator<ScanResultValue>
{
  private Yielder<ScanResultValue> yielder;
  private ScanQuery.ResultFormat resultFormat;

  public ScanQueryNoLimitRowIterator(
      QueryRunner<ScanResultValue> baseRunner,
      QueryPlus<ScanResultValue> queryPlus,
      Map<String, Object> responseContext
  )
  {
    ScanQuery query = Druids.ScanQueryBuilder.copy((ScanQuery) queryPlus.getQuery()).limit(Long.MAX_VALUE).timeOrder(
        ScanQuery.TimeOrder.NONE).build();
    resultFormat = query.getResultFormat();
    queryPlus = queryPlus.withQuery(query);
    Sequence<ScanResultValue> baseSequence = baseRunner.run(queryPlus, responseContext);
    yielder = baseSequence.toYielder(
        null,
        new YieldingAccumulator<ScanResultValue, ScanResultValue>()
        {
          @Override
          public ScanResultValue accumulate(ScanResultValue accumulated, ScanResultValue in)
          {
            yield();
            return in;
          }
        }
    );
  }

  @Override
  public boolean hasNext()
  {
    return !yielder.isDone();
  }

  @Override
  public ScanResultValue next()
  {
    ScanResultValue batch = yielder.get();
    if (ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat) ||
        ScanQuery.ResultFormat.RESULT_FORMAT_LIST.equals(resultFormat)) {
      yielder = yielder.next(null);
      return batch;
    }
    throw new UnsupportedOperationException(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {
    yielder.close();
  }
}
