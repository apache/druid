package tech.stackable.druid.opaauthorizer.opatypes;

import org.apache.druid.server.security.AuthenticationResult;

public class OpaInput {
  public AuthenticationResult authenticationResult;
  public String action;
  public OpaResource resource;

  public OpaInput(
      AuthenticationResult authenticationResult,
      String action,
      String resourceName,
      String resourceType) {
    this.authenticationResult = authenticationResult;
    this.action = action;
    this.resource = new OpaResource(resourceName, resourceType);
  }
}
