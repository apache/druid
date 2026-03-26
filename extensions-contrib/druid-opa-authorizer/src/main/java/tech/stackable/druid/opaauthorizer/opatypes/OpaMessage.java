package tech.stackable.druid.opaauthorizer.opatypes;

import org.apache.druid.server.security.AuthenticationResult;

public class OpaMessage {
  public OpaInput input;

  public OpaMessage(
      AuthenticationResult authenticationResult,
      String action,
      String resourceName,
      String resourceType) {
    this.input = new OpaInput(authenticationResult, action, resourceName, resourceType);
  }
}
