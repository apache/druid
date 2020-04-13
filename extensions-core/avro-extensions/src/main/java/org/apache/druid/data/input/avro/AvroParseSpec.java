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

package org.apache.druid.data.input.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.NestedDataParseSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.Parser;

public class AvroParseSpec extends NestedDataParseSpec<JSONPathSpec>
{
  @JsonCreator
  public AvroParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("flattenSpec") JSONPathSpec flattenSpec
  )
  {
    super(
        timestampSpec != null ? timestampSpec : new TimestampSpec(null, null, null),
        dimensionsSpec != null ? dimensionsSpec : DimensionsSpec.EMPTY,
        flattenSpec != null ? flattenSpec : JSONPathSpec.DEFAULT
    );
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    // makeParser is only used by StringInputRowParser, which cannot parse avro anyway.
    throw new UnsupportedOperationException("makeParser not supported");
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new AvroParseSpec(spec, getDimensionsSpec(), getFlattenSpec());
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new AvroParseSpec(getTimestampSpec(), spec, getFlattenSpec());
  }
}
