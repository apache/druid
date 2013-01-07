package com.metamx.druid.merger.coordinator.setup;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="classType")
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="galaxy", value=GalaxyUserData.class)
})
public interface WorkerUserData
{
}
