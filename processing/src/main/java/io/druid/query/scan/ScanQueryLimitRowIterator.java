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
package io.druid.query.scan;

import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ScanQueryLimitRowIterator implements CloseableIterator<ScanResultValue>
{
  private Yielder<ScanResultValue> yielder;
  private String resultFormat;
  private long limit = 0;
  private long count = 0;

  public ScanQueryLimitRowIterator(
      QueryRunner<ScanResultValue> baseRunner,
      QueryPlus<ScanResultValue> queryPlus,
      Map<String, Object> responseContext
  )
  {
    ScanQuery query = (ScanQuery) queryPlus.getQuery();
    resultFormat = query.getResultFormat();
    limit = query.getLimit();
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
    return !yielder.isDone() && count < limit;
  }

  @Override
  public ScanResultValue next()
  {
    ScanResultValue batch = yielder.get();
    if (ScanQuery.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat) ||
        ScanQuery.RESULT_FORMAT_LIST.equals(resultFormat)) {
      List events = (List) batch.getEvents();
      if (events.size() <= limit - count) {
        count += events.size();
        yielder = yielder.next(null);
        return batch;
      } else {
        // last batch
        // single batch length is <= Integer.MAX_VALUE, so this should not overflow
        int left = (int) (limit - count);
        count = limit;
        return new ScanResultValue(batch.getSegmentId(), batch.getColumns(), events.subList(0, left));
      }
    }
    throw new UnsupportedOperationException(ScanQuery.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
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
