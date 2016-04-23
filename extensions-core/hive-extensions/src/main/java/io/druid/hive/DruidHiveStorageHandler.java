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

package io.druid.hive;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

/**
 */
public class DruidHiveStorageHandler extends DefaultStorageHandler
{
  @Override
  public Class<? extends InputFormat> getInputFormatClass()
  {
    return DruidHiveInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass()
  {
    return DruidHiveInputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass()
  {
    return DruidHiveSerDe.class;
  }
}
