package org.apache.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskMasterConfig {
    @JsonProperty
    private boolean delegateRequestsToFollowers;

    @JsonCreator
    public TaskMasterConfig(
        @JsonProperty("delegateRequestsToFollowers") final Boolean delegateRequestsToFollowers
    )
    {
      this.delegateRequestsToFollowers = delegateRequestsToFollowers == null ? false : delegateRequestsToFollowers;
    }

    public boolean getDelegateRequestsToFollowers()
    {
      return delegateRequestsToFollowers;
    }
}
