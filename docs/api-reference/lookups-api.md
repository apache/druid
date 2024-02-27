---
id: lookups-api
title: Lookups API
sidebar_label: Lookups
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

This document describes the API endpoints to configure, update, retrieve, and manage lookups for Apache Druid.

## Configure lookups

### Bulk update
Lookups can be updated in bulk by posting a JSON object to `/druid/coordinator/v1/lookups/config`. The format of the json object is as follows:

```json
{
    "<tierName>": {
        "<lookupName>": {
          "version": "<version>",
          "lookupExtractorFactory": {
            "type": "<someExtractorFactoryType>",
            "<someExtractorField>": "<someExtractorValue>"
          }
        }
    }
}
```

Note that "version" is an arbitrary string assigned by the user, when making updates to existing lookup then user would need to specify a lexicographically higher version.

For example, a config might look something like:

```json
{
  "__default": {
    "country_code": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "77483": "United States"
        }
      }
    },
    "site_id": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "cachedNamespace",
        "extractionNamespace": {
          "type": "jdbc",
          "connectorConfig": {
            "createTables": true,
            "connectURI": "jdbc:mysql:\/\/localhost:3306\/druid",
            "user": "druid",
            "password": "diurd"
          },
          "table": "lookupTable",
          "keyColumn": "country_id",
          "valueColumn": "country_name",
          "tsColumn": "timeColumn"
        },
        "firstCacheTimeout": 120000,
        "injective": true
      }
    },
    "site_id_customer1": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "847632": "Internal Use Only"
        }
      }
    },
    "site_id_customer2": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "AHF77": "Home"
        }
      }
    }
  },
  "realtime_customer1": {
    "country_code": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "77483": "United States"
        }
      }
    },
    "site_id_customer1": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "847632": "Internal Use Only"
        }
      }
    }
  },
  "realtime_customer2": {
    "country_code": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "77483": "United States"
        }
      }
    },
    "site_id_customer2": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "map",
        "map": {
          "AHF77": "Home"
        }
      }
    }
  }
}
```

All entries in the map will UPDATE existing entries. No entries will be deleted.

### Update lookup

A `POST` to a particular lookup extractor factory via `/druid/coordinator/v1/lookups/config/{tier}/{id}` creates or updates that specific extractor factory.

For example, a post to `/druid/coordinator/v1/lookups/config/realtime_customer1/site_id_customer1` might contain the following:

```json
{
  "version": "v1",
  "lookupExtractorFactory": {
    "type": "map",
    "map": {
      "847632": "Internal Use Only"
    }
  }
}
```

This will replace the `site_id_customer1` lookup in the `realtime_customer1` with the definition above.

Assign a unique version identifier each time you update a lookup extractor factory. Otherwise the call will fail.

### Get all lookups

A `GET` to `/druid/coordinator/v1/lookups/config/all` will return all known lookup specs for all tiers.

### Get lookup

A `GET` to a particular lookup extractor factory is accomplished via `/druid/coordinator/v1/lookups/config/{tier}/{id}`

Using the prior example, a `GET` to `/druid/coordinator/v1/lookups/config/realtime_customer2/site_id_customer2` should return

```json
{
  "version": "v1",
  "lookupExtractorFactory": {
    "type": "map",
    "map": {
      "AHF77": "Home"
    }
  }
}
```

### Delete lookup

A `DELETE` to `/druid/coordinator/v1/lookups/config/{tier}/{id}` will remove that lookup from the cluster. If it was last lookup in the tier, then tier is deleted as well.

### Delete tier

A `DELETE` to `/druid/coordinator/v1/lookups/config/{tier}` will remove that tier from the cluster.

### List tier names

A `GET` to `/druid/coordinator/v1/lookups/config` will return a list of known tier names in the dynamic configuration.
To discover a list of tiers currently active in the cluster in addition to ones known in the dynamic configuration, the parameter `discover=true` can be added as per `/druid/coordinator/v1/lookups/config?discover=true`.

### List lookup names

A `GET` to `/druid/coordinator/v1/lookups/config/{tier}` will return a list of known lookup names for that tier.

These end points can be used to get the propagation status of configured lookups to processes using lookups such as Historicals.

## Lookup status

### List load status of all lookups

`GET /druid/coordinator/v1/lookups/status` with optional query parameter `detailed`.

### List load status of lookups in a tier

`GET /druid/coordinator/v1/lookups/status/{tier}` with optional query parameter `detailed`.

### List load status of single lookup

`GET /druid/coordinator/v1/lookups/status/{tier}/{lookup}` with optional query parameter `detailed`.

### List lookup state of all processes

`GET /druid/coordinator/v1/lookups/nodeStatus` with optional query parameter `discover` to discover tiers advertised by other Druid nodes, or by default, returning all configured lookup tiers. The default response will also include the lookups which are loaded, being loaded, or being dropped on each node, for each tier, including the complete lookup spec. Add the optional query parameter `detailed=false` to only include the 'version' of the lookup instead of the complete spec.

### List lookup state of processes in a tier

`GET /druid/coordinator/v1/lookups/nodeStatus/{tier}`

### List lookup state of single process

`GET /druid/coordinator/v1/lookups/nodeStatus/{tier}/{host:port}`

## Internal API

The Peon, Router, Broker, and Historical processes all have the ability to consume lookup configuration.
There is an internal API these processes use to list/load/drop their lookups starting at `/druid/listen/v1/lookups`.
These follow the same convention for return values as the cluster wide dynamic configuration. Following endpoints
can be used for debugging purposes but not otherwise.

### Get lookups

A `GET` to the process at `/druid/listen/v1/lookups` will return a json map of all the lookups currently active on the process.
The return value will be a json map of the lookups to their extractor factories.

```json
{
  "site_id_customer2": {
    "version": "v1",
    "lookupExtractorFactory": {
      "type": "map",
      "map": {
        "AHF77": "Home"
      }
    }
  }
}
```

### Get lookup

A `GET` to the process at `/druid/listen/v1/lookups/some_lookup_name` will return the LookupExtractorFactory for the lookup identified by `some_lookup_name`.
The return value will be the json representation of the factory.

```json
{
  "version": "v1",
  "lookupExtractorFactory": {
    "type": "map",
    "map": {
      "AHF77": "Home"
    }
  }
}
```