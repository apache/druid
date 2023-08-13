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

package org.apache.druid.segment.serde;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.Indexed;

import java.io.IOException;

/**
 * A {@link ColumnarInts} facade over {@link CombineFirstTwoValuesIndexedInts}.
 *
 * Provided to enable compatibility for segments written under {@link NullHandling#sqlCompatible()} mode but
 * read under {@link NullHandling#replaceWithDefault()} mode.
 *
 * @see NullHandling#mustCombineNullAndEmptyInDictionary(Indexed)
 */
public class CombineFirstTwoValuesColumnarInts extends CombineFirstTwoValuesIndexedInts implements ColumnarInts
{
  public CombineFirstTwoValuesColumnarInts(ColumnarInts delegate)
  {
    super(delegate);
  }

  @Override
  public void close() throws IOException
  {
    ((ColumnarInts) delegate).close();
  }
}
