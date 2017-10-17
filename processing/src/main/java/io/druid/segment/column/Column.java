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

package io.druid.segment.column;

import io.druid.java.util.common.StringUtils;

/**
 */
public interface Column
{
  public static final String TIME_COLUMN_NAME = "__time";
  public ColumnCapabilities getCapabilities();

  String DOUBLE_STORAGE_TYPE_PROPERTY = "druid.indexing.doubleStorage";
  static boolean storeDoubleAsFloat()
  {
    String value = System.getProperty(DOUBLE_STORAGE_TYPE_PROPERTY, "float");
    return !StringUtils.toLowerCase(value).equals("double");
  }

  public int getLength();
  public DictionaryEncodedColumn getDictionaryEncoding();
  public RunLengthColumn getRunLengthColumn();
  public GenericColumn getGenericColumn();
  public ComplexColumn getComplexColumn();
  public BitmapIndex getBitmapIndex();
  public SpatialIndex getSpatialIndex();
}
