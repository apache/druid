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

package org.apache.druid.indexer.hbase2;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.indexer.hbase2.input.HBaseInputRowParser;
import org.apache.druid.indexer.hbase2.input.HBaseParseSpec;
import org.apache.druid.indexer.hbase2.path.HBasePathSpec;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class HBaseIndexTaskModule implements DruidModule
{

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(new SimpleModule(getClass().getSimpleName()).registerSubtypes(
        new NamedType(HBaseInputRowParser.class, "hbase2"),
        new NamedType(HBaseParseSpec.class, "hbase2"),
        new NamedType(HBaseFirehoseFactory.class, "hbase2"),
        new NamedType(HadoopHBaseIndexTask.class, "index_hadoop_hbase2"),
        new NamedType(HBasePathSpec.class, "hbase2")));
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
