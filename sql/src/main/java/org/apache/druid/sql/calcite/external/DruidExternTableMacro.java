package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.NlsString;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;

public class DruidExternTableMacro extends DruidUserDefinedTableMacro
{
  private static final Logger LOG = new Logger(DruidExternTableMacro.class);
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

    if (inputSourceStr == null) {
      // this shouldn't happen, the input source paraemeter should have been validated before this
      return Collections.singleton(Externals.EXTERNAL_RESOURCE_ACTION);
    }

    try {
      JsonNode jsonNode = ((DruidTableMacro) macro).getJsonMapper().readTree(inputSourceStr);
      return Collections.singleton(new ResourceAction(new Resource(
          ResourceType.EXTERNAL,
          jsonNode.get("type").asText()
      ), Action.READ));
    }
    catch (JsonProcessingException e) {
      // this shouldn't happen, the input source paraemeter should have been validated before this
      LOG.error(e, "Error when serializing inputSource parameter found in EXTERN macro");
    }
    return Collections.singleton(Externals.EXTERNAL_RESOURCE_ACTION);
  }

  @Nullable
  private String getInputSourceArgument(final SqlCall call) {
    if (call.getOperandList().size() > 0) {
      if (call.getOperandList().get(0) instanceof SqlCharStringLiteral) {
        return ((SqlCharStringLiteral) call.getOperandList().get(0)).toValue();
      }
    }
    for (SqlNode sqlNode : call.getOperandList()) {
      if (sqlNode instanceof SqlCall) {
        String argumentName =((SqlCall) sqlNode).getOperandList().size() > 1 ?
                             ((SqlCall) sqlNode).getOperandList().get(1).toString()
                             : null;
        if (ExternalOperatorConversion.INPUT_SOURCE_PARAM.equals(argumentName)) {
          return ((NlsString)((SqlCharStringLiteral)((SqlCall) call.getOperandList().get(0))
              .getOperandList()
              .get(0))
              .getValue())
              .getValue();
        }
      }
    }
    return null;
  }
}
