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

package io.druid.segment;

import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RoaringBitmapIndexMergerV9Test extends IndexMergerTestBase
{
  public RoaringBitmapIndexMergerV9Test(
      CompressionStrategy compressionStrategy,
      CompressionStrategy dimCompressionStrategy,
      LongEncodingStrategy longEncodingStrategy
  )
  {
    super(new RoaringBitmapSerdeFactory(null), compressionStrategy, dimCompressionStrategy, longEncodingStrategy);
    indexMerger = TestHelper.getTestIndexMergerV9();
  }
}
