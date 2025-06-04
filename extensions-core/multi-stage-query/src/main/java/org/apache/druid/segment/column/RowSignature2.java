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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Collections;
import java.util.List;

public class RowSignature2 extends RowSignature
{
  public RowSignature2(final List<ColumnSignature> columnTypeList)
  {
    super(columnTypeList);
  }

  public static RowSignature2 empty()
  {
    return new RowSignature2(Collections.emptyList());
  }

  public static RowSignature2 of(RowSignature signature)
  {
    return new RowSignature2(signature.asColumnSignatures());
  }

  @JsonValue(false)
  protected List<ColumnSignature> asColumnSignatures() {
    return super.asColumnSignatures();
  }


  @JsonValue
  protected List<Object> asColumnSignatures2() {
    return super.asColumnSignatures();
  }

}
