---
layout: doc_page
---

# Druid examples

## TwitterSpritzerFirehose

This firehose connects directly to the twitter spritzer data stream.

Sample spec:

```json
"firehose" : {
    "type" : "twitzer",
    "maxEventCount": -1,
    "maxRunMinutes": 0
}
```

|property|description|default|required?|
|--------|-----------|-------|---------|
|type|This should be "twitzer"|N/A|yes|
|maxEventCount|max events to receive, -1 is infinite, 0 means nothing is delivered; use this to prevent infinite space consumption or to prevent getting throttled at an inconvenient time.|N/A|yes|
|maxRunMinutes|maximum number of minutes to fetch Twitter events.  Use this to prevent getting throttled at an inconvenient time. If zero or less, no time limit for run.|N/A|yes|
