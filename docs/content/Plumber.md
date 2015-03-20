---
layout: doc_page
---

# Druid Plumbers

The plumber handles generated segments both while they are being generated and when they are "done". This is also technically a pluggable interface and there are multiple implementations. However, plumbers handle numerous complex details, and therefore an advanced understanding of Druid is recommended before implementing your own.

Available Plumbers
------------------

#### YeOldePlumber

This plumber creates single historical segments.

#### RealtimePlumber

This plumber creates real-time/mutable segments.
