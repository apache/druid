package tech.stackable.druid.opaauthorizer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;
import tech.stackable.druid.opaauthorizer.opatypes.OpaMessage;
import tech.stackable.druid.opaauthorizer.opatypes.OpaResponse;

@JsonTypeName("opa")
public class OpaAuthorizer implements Authorizer {
  private static final Logger LOG = new Logger(OpaAuthorizer.class);
  private final String opaUri;
  private final ObjectMapper objectMapper;

  @JsonCreator
  public OpaAuthorizer(@JsonProperty("name") String name, @JsonProperty("opaUri") String opaUri) {
    this.opaUri = opaUri;
    objectMapper =
        new ObjectMapper()
            // https://github.com/stackabletech/druid-opa-authorizer/issues/72
            // OPA server can send other fields, such as `decision_id`` when enabling decision logs
            // We could add all the fields we *currently* know, but it's more future-proof to ignore
            // any unknown fields.
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public Access authorize(
      AuthenticationResult authenticationResult, Resource resource, Action action) {
    LOG.debug(
        "Authorizing %s for %s on %s",
        authenticationResult.getIdentity(), action.name(), resource.toString());
    LOG.trace("Creating OPA request JSON.");
    OpaMessage msg =
        new OpaMessage(authenticationResult, action.name(), resource.getName(), resource.getType());
    String msgJson;
    try {
      msgJson = objectMapper.writeValueAsString(msg);
    } catch (JsonProcessingException e) {
      return new Access(false, "Failed to create the OPA request JSON: " + e);
    }

    LOG.trace("Creating HTTP Client and executing post.");
    var client = HttpClient.newHttpClient();
    try {
      var request =
          HttpRequest.newBuilder()
              .uri(new URI(opaUri))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(msgJson))
              .build();

      var response = client.send(request, HttpResponse.BodyHandlers.ofString());

      LOG.debug("OPA Response code: %s - %s", response.statusCode(), response.body());
      LOG.trace("Parsing OPA response.");
      OpaResponse opaResponse = objectMapper.readValue(response.body(), OpaResponse.class);
      if (opaResponse.result) {
        return Access.OK;
      } else {
        return new Access(false, "Access denied.");
      }

    } catch (Exception e) {
      return new Access(false, "An error occurred: " + e);
    }
  }
}
