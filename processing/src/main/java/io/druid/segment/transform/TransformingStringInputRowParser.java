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
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class TransformingStringInputRowParser extends StringInputRowParser
{
  private final TransformSpec transformSpec;
  private final Transformer transformer;

  public TransformingStringInputRowParser(
      final ParseSpec parseSpec,
      final String encoding,
      final TransformSpec transformSpec
  )
  {
    super(parseSpec, encoding);
    this.transformSpec = transformSpec;
    this.transformer = transformSpec.toTransformer();
  }

  @Override
  public InputRow parse(final ByteBuffer input)
  {
    return transformer.transform(super.parse(input));
  }

  @Nullable
  @Override
  public InputRow parse(@Nullable final String input)
  {
    return transformer.transform(super.parse(input));
  }

  @Override
  public StringInputRowParser withParseSpec(final ParseSpec parseSpec)
  {
    return new TransformingStringInputRowParser(parseSpec, getEncoding(), transformSpec);
  }

  public TransformSpec getTransformSpec()
  {
    return transformSpec;
  }
}
