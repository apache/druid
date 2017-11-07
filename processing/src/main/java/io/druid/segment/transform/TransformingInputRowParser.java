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

package io.druid.segment.transform;

import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;

public class TransformingInputRowParser<T> implements InputRowParser<T>
{
  private final InputRowParser<T> parser;
  private final TransformSpec transformSpec;
  private final Transformer transformer;

  public TransformingInputRowParser(final InputRowParser<T> parser, final TransformSpec transformSpec)
  {
    this.parser = parser;
    this.transformSpec = transformSpec;
    this.transformer = transformSpec.toTransformer();
  }

  public InputRowParser<T> getParser()
  {
    return parser;
  }

  @Override
  public InputRow parse(final T row)
  {
    return transformer.transform(parser.parse(row));
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parser.getParseSpec();
  }

  @Override
  @SuppressWarnings("unchecked")
  public InputRowParser<T> withParseSpec(final ParseSpec parseSpec)
  {
    return new TransformingInputRowParser<>(parser.withParseSpec(parseSpec), transformSpec);
  }

  public TransformSpec getTransformSpec()
  {
    return transformSpec;
  }
}
