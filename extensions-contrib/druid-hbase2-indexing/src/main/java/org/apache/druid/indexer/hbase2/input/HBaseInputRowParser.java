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

package org.apache.druid.indexer.hbase2.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.indexer.hbase2.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class HBaseInputRowParser implements InputRowParser<Result>
{
  private static final Logger LOG = LogManager.getLogger(HBaseInputRowParser.class);

  private final ParseSpec parseSpec;
  private final HBaseRowSpec hbaseRowSpec;

  private final MapInputRowParser mapParser;

  private final HBaseRowKeySpec rowKeySpec;

  private final List<HBaseColumnSchema> columnSchemaList;

  @JsonCreator
  public HBaseInputRowParser(@JsonProperty("parseSpec") HBaseParseSpec parseSpec)
  {
    this.parseSpec = Preconditions.checkNotNull(parseSpec, "parseSpec");
    this.mapParser = new MapInputRowParser(parseSpec);

    hbaseRowSpec = parseSpec.getHbaseRowSpec();
    rowKeySpec = hbaseRowSpec.getRowKeySpec();
    columnSchemaList = hbaseRowSpec.getColumnSchemaList();
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public HBaseInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new HBaseInputRowParser((HBaseParseSpec) parseSpec);
  }

  @Override
  public List<InputRow> parseBatch(Result input)
  {
    return Lists.newArrayList(parse(input));
  }

  @Override
  public InputRow parse(Result input)
  {
    // All values in Result are stored in a Map and passed to MapParser.
    Map<String, Object> resultMap = rowKeySpec.getRowKeyColumns(input);

    columnSchemaList.stream().forEach(cs -> {
      resultMap.put(cs.getMappingName(), HBaseUtil.getColumnValue(input, cs));
    });

    return parseMap(resultMap);
  }

  @Nullable
  private InputRow parseMap(@Nullable Map<String, Object> theMap)
  {
    if (theMap == null) {
      return null;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("input: {}", theMap);
    }

    return Iterators.getOnlyElement(mapParser.parseBatch(theMap).iterator());
  }
}
