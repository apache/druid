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

package org.apache.druid.common;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.NoopInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;

public class ProcessingTestToolbox
{
  private final ObjectMapper jsonMapper;
  private final IndexMergerV9 indexMerger;
  private final IndexIO indexIO;

  public ProcessingTestToolbox()
  {
    jsonMapper = new DefaultObjectMapper();
    indexIO = new IndexIO(
        jsonMapper,
        () -> 0
    );
    indexMerger = new IndexMergerV9(jsonMapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());

    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class, TestExprMacroTable.INSTANCE)
            .addValue(IndexIO.class, indexIO)
            .addValue(ObjectMapper.class, jsonMapper)
            .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT)
    );

    jsonMapper.registerModule(
        new SimpleModule()
        {
          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(
                new NamedType(NoopInputSource.class, "noop"),
                new NamedType(NoopInputFormat.class, "noop")
            );
          }
        }
    );
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  public IndexMergerV9 getIndexMerger()
  {
    return indexMerger;
  }

  public IndexIO getIndexIO()
  {
    return indexIO;
  }
}
