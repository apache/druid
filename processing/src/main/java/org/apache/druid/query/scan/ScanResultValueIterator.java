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
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;

/**
 * Iterates over the scan result sequence and provides an interface to clean up the resources (if any) to close the
 * underlying sequence. Similar to {@link Yielder}, once close is called on the iterator, the calls to the rest of the
 * iterator's methods are undefined.
 */
public class ScanResultValueIterator implements CloseableIterator<ScanResultValue>
{
  Yielder<ScanResultValue> yielder;

  public ScanResultValueIterator(final Sequence<ScanResultValue> resultSequence)
  {
    yielder = Yielders.each(resultSequence);
  }

  @Override
  public void close() throws IOException
  {
    yielder.close();
  }

  @Override
  public boolean hasNext()
  {
    return !yielder.isDone();
  }

  @Override
  public ScanResultValue next()
  {
    ScanResultValue scanResultValue = yielder.get();
    yielder = yielder.next(null);
    return scanResultValue;
  }
}
