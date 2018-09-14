---
layout: doc_page
---

# Deleting Data

Permanent deletion of a Druid segment has two steps:

1. The segment must first be marked as "unused". This occurs when a segment is dropped by retention rules, and when a user manually disables a segment through the Coordinator API.
2. After segments have been marked as "unused", a Kill Task will delete any "unused" segments from Druid's metadata store as well as deep storage.

For documentation on retention rules, please see [Data Retention](../operations/rule-configuration.html).

For documentation on disabling segments using the Coordinator API, please see [Coordinator Delete API](../operations/api-reference.html#coordinator-delete)

A data deletion tutorial is available at [Tutorial: Deleting data](../tutorials/tutorial-delete-data.html)

## Kill Task

Kill tasks delete all information about a segment and removes it from deep storage. Killable segments must be disabled (used==0) in the Druid segment table. The available grammar is:

```json
{
    "type": "kill",
    "id": <task_id>,
    "dataSource": <task_datasource>,
    "interval" : <all_segments_in_this_interval_will_die!>,
    "context": <task context>
}
```
