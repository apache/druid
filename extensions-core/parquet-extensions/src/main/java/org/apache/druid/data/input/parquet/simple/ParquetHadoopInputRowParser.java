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

package org.apache.druid.data.input.parquet.simple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.parquet.example.data.Group;

import java.util.List;

public class ParquetHadoopInputRowParser implements InputRowParser<Group>
{
  private final ParseSpec parseSpec;
  private final boolean binaryAsString;
  private final ObjectFlattener<Group> groupFlattener;
  private final MapInputRowParser parser;

  @JsonCreator
  public ParquetHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("binaryAsString") Boolean binaryAsString
  )
  {
    this.parseSpec = parseSpec;
    this.binaryAsString = binaryAsString == null ? false : binaryAsString;

    final JSONPathSpec flattenSpec;
    if ((parseSpec instanceof ParquetParseSpec)) {
      flattenSpec = ((ParquetParseSpec) parseSpec).getFlattenSpec();
    } else {
      flattenSpec = JSONPathSpec.DEFAULT;
    }
    this.groupFlattener = ObjectFlatteners.create(flattenSpec, new ParquetGroupFlattenerMaker(this.binaryAsString));
    this.parser = new MapInputRowParser(parseSpec);
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ParquetHadoopInputRowParser(parseSpec, binaryAsString);
  }

  @Override
  public List<InputRow> parseBatch(Group group)
  {
    return parser.parseBatch(groupFlattener.flatten(group));
  }
}
