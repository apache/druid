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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.initialization.DruidModule;

/**
 * This {@link ExtensionPoint} allows for coordinator duty to be pluggable
 * so that users can register their own duties without modifying Core Druid classes.
 *
 * Users can write their own custom coordinator duty implemnting this interface and setting the {@link JsonTypeName}.
 * Next, users will need to register their custom coordinator as subtypes in their
 * Module's {@link DruidModule#getJacksonModules()}. Once these steps are done, user will be able to load their
 * custom coordinator duty using the following properties:
 * druid.coordinator.dutyGroups=[<GROUP_NAME>]
 * druid.coordinator.<GROUP_NAME>.duties=[<DUTY_NAME_MATCHING_JSON_TYPE_NAME>]
 * druid.coordinator.<GROUP_NAME>.duty.<DUTY_NAME_MATCHING_JSON_TYPE_NAME>.<SOME_CONFIG_1>=100
 * druid.coordinator.<GROUP_NAME>.duty.<DUTY_NAME_MATCHING_JSON_TYPE_NAME>.<SOME_CONFIG_2>=200
 * druid.coordinator.<GROUP_NAME>.period="P1D"
 *
 * The duties can be grouped into multiple groups as per the elements in list druid.coordinator.dutyGroups.
 * All duties in the same group will have the same run period configured by druid.coordinator.<GROUP_NAME>.period.
 * There will be a single thread running the duties sequentially for each group.
 *
 * Note that custom duty does not implement CoordinatorDuty directly as existing Core Druid Coordinator Duties
 * don't have associated JSON type and should not be manually grouped/enabled/disabled by the users.
 * (The only exception is the metadata cleanup duties which we may refactor to use the custom duty system in the future)
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "killSupervisors", value = KillSupervisorsCustomDuty.class),
    @JsonSubTypes.Type(name = "compactSegments", value = CompactSegments.class),
})
@ExtensionPoint
public interface CoordinatorCustomDuty extends CoordinatorDuty
{

}
