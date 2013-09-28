---
layout: doc_page
---
Tasks are run on workers and always operate on a single datasource. Once an indexer coordinator node accepts a task, a lock is created for the datasource and interval specified in the task. Tasks do not need to explicitly release locks, they are released upon task completion. Tasks may potentially release locks early if they desire. Tasks ids are unique by naming them using UUIDs or the timestamp in which the task was created. Tasks are also part of a "task group", which is a set of tasks that can share interval locks.

There are several different types of tasks.

Append Task
-----------

Append tasks append a list of segments together into a single segment (one after the other). The grammar is:

    {
        "id": <task_id>,
        "dataSource": <task_datasource>,
        "segments": <JSON list of DataSegment objects to append> 
    }

Merge Task
----------

Merge tasks merge a list of segments together. Any common timestamps are merged. The grammar is:

    {
        "id": <task_id>,
        "dataSource": <task_datasource>,
        "segments": <JSON list of DataSegment objects to append> 
    }

Delete Task
-----------

Delete tasks create empty segments with no data. The grammar is:

    {
        "id": <task_id>,
        "dataSource": <task_datasource>,
        "segments": <JSON list of DataSegment objects to append> 
    }

Kill Task
---------

Kill tasks delete all information about a segment and removes it from deep storage. Killable segments must be disabled (used==0) in the Druid segment table. The available grammar is:

    {
        "id": <task_id>,
        "dataSource": <task_datasource>,
        "segments": <JSON list of DataSegment objects to append> 
    }

Index Task
----------

Index Partitions Task
---------------------

Index Generator Task
--------------------

Index Hadoop Task
-----------------

Index Realtime Task
-------------------

Version Converter Task
----------------------

Version Converter SubTask
-------------------------
