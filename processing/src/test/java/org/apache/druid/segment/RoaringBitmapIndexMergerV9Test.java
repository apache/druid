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

package org.apache.druid.segment;

import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RoaringBitmapIndexMergerV9Test extends IndexMergerTestBase
{
  public RoaringBitmapIndexMergerV9Test(
      CompressionStrategy compressionStrategy,
      CompressionStrategy dimCompressionStrategy,
      LongEncodingStrategy longEncodingStrategy,
      SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  )
  {
    super(
        new RoaringBitmapSerdeFactory(null),
        compressionStrategy,
        dimCompressionStrategy,
        longEncodingStrategy
    );
    indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
  }
}
