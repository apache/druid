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

import com.metamx.collections.bitmap.BitmapFactory;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 */
public interface QueryableIndex extends ColumnSelector, Closeable
{
  public Interval getDataInterval();
  public int getNumRows();
  public Indexed<String> getAvailableDimensions();
  public BitmapFactory getBitmapFactoryForDimensions();
  public Metadata getMetadata();
  public Map<String, DimensionHandler> getDimensionHandlers();

  /**
   * The close method shouldn't actually be here as this is nasty. We will adjust it in the future.
   * @throws java.io.IOException if an exception was thrown closing the index
   */
  //@Deprecated // This is still required for SimpleQueryableIndex. It should not go away unitl SimpleQueryableIndex is fixed
  public void close() throws IOException;
}
