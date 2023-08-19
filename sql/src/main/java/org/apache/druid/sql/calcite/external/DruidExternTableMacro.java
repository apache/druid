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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.NlsString;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.table.DruidTable;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used by {@link ExternalOperatorConversion} to generate a {@link DruidTable}
 * that references an {@link ExternalDataSource}.
 */
public class DruidExternTableMacro extends DruidUserDefinedTableMacro
{
  public DruidExternTableMacro(DruidTableMacro macro)
  {
    super(macro);
  }

  @Override
  public Set<ResourceAction> computeResources(final SqlCall call, boolean inputSourceTypeSecurityEnabled)
  {
    if (!inputSourceTypeSecurityEnabled) {
      return Collections.singleton(Externals.EXTERNAL_RESOURCE_ACTION);
    }
    String inputSourceStr = getInputSourceArgument(call);

    try {
      InputSource inputSource = ((DruidTableMacro) macro).getJsonMapper().readValue(inputSourceStr, InputSource.class);
      return inputSource.getTypes().stream()
          .map(inputSourceType -> new ResourceAction(new Resource(inputSourceType, ResourceType.EXTERNAL), Action.READ))
          .collect(Collectors.toSet());
    }
    catch (JsonProcessingException e) {
      // this shouldn't happen, the input source paraemeter should have been validated before this
      throw new RuntimeException(e);
    }
  }

  @NotNull
  private String getInputSourceArgument(final SqlCall call)
  {
    // this covers case where parameters are used positionally
    if (call.getOperandList().size() > 0) {
      if (call.getOperandList().get(0) instanceof SqlCharStringLiteral) {
        return ((SqlCharStringLiteral) call.getOperandList().get(0)).toValue();
      }
    }

    // this covers case where named parameters are used.
    for (SqlNode sqlNode : call.getOperandList()) {
      if (sqlNode instanceof SqlCall) {
        String argumentName = ((SqlCall) sqlNode).getOperandList().size() > 1 ?
                             ((SqlCall) sqlNode).getOperandList().get(1).toString()
                             : null;
        if (ExternalOperatorConversion.INPUT_SOURCE_PARAM.equals(argumentName)) {
          return ((NlsString) ((SqlCharStringLiteral) ((SqlCall) call.getOperandList().get(0))
              .getOperandList()
              .get(0))
              .getValue())
              .getValue();
        }
      }
    }
    // this shouldn't happen, as the sqlCall should have been validated by this point,
    // and be guarenteed to have this parameter.
    throw new RuntimeException("inputSource parameter not found in extern function");
  }
}
