---
id: gce-extensions
title: "GCE Extensions"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


To use this Apache Druid (incubating) extension, make sure to [include](../../development/extensions.md#loading-extensions) `gce-extensions`.

At the moment, this extension enables only Druid to autoscale instances in GCE.

The extension manages the instances to be scaled up and down through the use of the [Managed Instance Groups](https://cloud.google.com/compute/docs/instance-groups/creating-groups-of-managed-instances#resize_managed_group)
of GCE (MIG from now on). This choice has been made to ease the configuration of the machines and simplify their
management.

For this reason, in order to use this extension, the user must have created
1. An instance template with the right machine type and image to bu used to run the MiddleManager
2. A MIG that has been configured to use the instance template created in the point above

Moreover, in order to be able to rescale the machines in the MIG, the Overlord must run with a service account
guaranteeing the following two scopes from the [Compute Engine API](https://developers.google.com/identity/protocols/googlescopes#computev1)
- `https://www.googleapis.com/auth/cloud-platform`
- `https://www.googleapis.com/auth/compute`

## Overlord Dynamic Configuration

The Overlord can dynamically change worker behavior.

The JSON object can be submitted to the Overlord via a POST request at:

```
http://<OVERLORD_IP>:<port>/druid/indexer/v1/worker
```

Optional Header Parameters for auditing the config change can also be specified.

|Header Param Name| Description | Default |
|----------|-------------|---------|
|`X-Druid-Author`| author making the config change|""|
|`X-Druid-Comment`| comment describing the change being done|""|

A sample worker config spec is shown below:

```json
{
  "autoScaler": {
    "envConfig" : {
      "numInstances" : 1,
      "projectId" : "super-project",
      "zoneName" : "us-central-1",
      "managedInstanceGroupName" : "druid-middlemanagers"
    },
    "maxNumWorkers" : 4,
    "minNumWorkers" : 2,
    "type" : "gce"
  }
}
```

The configuration of the autoscaler is quite simple and it is made of two levels only.

The external level specifies the `type`—always `gce` in this case— and two numeric values,
the `maxNumWorkers` and `minNumWorkers` used to define the boundaries in between which the
number of instances must be at any time.

The internal level is the `envConfig` and it is used to specify

- The `numInstances` used to specify how many workers will be spawned at each 
request to provision more workers.  This is safe to be left to `1`
- The `projectId` used to specify the name of the project in which the MIG resides
- The `zoneName` used to identify in which zone of the worlds the MIG is
- The `managedInstanceGroupName` used to specify the MIG containing the instances created or 
removed

Please refer to the Overlord Dynamic Configuration section in the main [documentation](../../configuration/index.md)
for parameters other than the ones specified here, such as `selectStrategy` etc.

## Known limitations

- The module internally uses the [ListManagedInstances](https://cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers/listManagedInstances)
 call from the API and, while the documentation of the API states that the call can be paged through using the
 `pageToken` argument, the responses to such call do not provide any `nextPageToken` to set such parameter. This means
 that the extension can operate safely with a maximum of 500 MiddleManagers instances at any time (the maximum number
 of instances to be returned for each call).
 