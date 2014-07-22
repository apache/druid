package io.druid.server.router;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;
import io.druid.query.Query;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "timeBoundary", value = TimeBoundaryTieredBrokerSelectorStrategy.class),
    @JsonSubTypes.Type(name = "priority", value = PriorityTieredBrokerSelectorStrategy.class)
})

public interface TieredBrokerSelectorStrategy
{
  public Optional<String> getBrokerServiceName(TieredBrokerConfig config, Query query);
}
