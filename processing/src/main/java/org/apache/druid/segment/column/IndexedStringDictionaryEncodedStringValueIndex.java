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

package org.apache.druid.segment.column;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;

public final class IndexedStringDictionaryEncodedStringValueIndex<T extends Indexed<String>>
    extends BaseIndexedDictionaryEncodedIndex<String, T> implements DictionaryEncodedStringValueIndex
{
  public IndexedStringDictionaryEncodedStringValueIndex(
      BitmapFactory bitmapFactory,
      T dictionary,
      Indexed<ImmutableBitmap> bitmaps
  )
  {
    super(bitmapFactory, dictionary, bitmaps);
  }

  @Override
  public int getCardinality()
  {
    return dictionary.size();
  }

  @Nullable
  @Override
  public String getValue(int index)
  {
    return dictionary.get(index);
  }
}
