package com.metamx.druid.merger.coordinator.setup;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type")
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="ec2", value=EC2NodeData.class)
})
public interface WorkerNodeData
{
}
