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

package org.apache.druid.data.input.orc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.orc.mapred.OrcStruct;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;

public class OrcHadoopInputRowParser implements InputRowParser<OrcStruct>
{
  private final ParseSpec parseSpec;
  private final ObjectFlattener<OrcStruct> orcStructFlattener;
  private final MapInputRowParser parser;
  private final boolean binaryAsString;

  @JsonCreator
  public OrcHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("binaryAsString") @Nullable Boolean binaryAsString
  )
  {
    this.parseSpec = parseSpec;
    this.binaryAsString = binaryAsString == null ? false : binaryAsString;
    final JSONPathSpec flattenSpec;
    if (parseSpec instanceof OrcParseSpec) {
      flattenSpec = ((OrcParseSpec) parseSpec).getFlattenSpec();
    } else {
      flattenSpec = JSONPathSpec.DEFAULT;
    }
    this.orcStructFlattener = ObjectFlatteners.create(flattenSpec, new OrcStructFlattenerMaker(this.binaryAsString));
    this.parser = new MapInputRowParser(parseSpec);
  }

  @NotNull
  @Override
  public List<InputRow> parseBatch(OrcStruct input)
  {
    return parser.parseBatch(orcStructFlattener.flatten(input));
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new OrcHadoopInputRowParser(parseSpec, binaryAsString);
  }
}
