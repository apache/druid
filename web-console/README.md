# Apache Druid web console

This is the unified Druid web console that servers as a data management layer for Druid.

## How to watch and run for development

1. Install the modules with `npm install`
2. Run `npm start`


## Description of the directory structure

A lot of the directory structure was created to preserve the legacy console structure as much as possible.

- `fonts/` - Fonts used by bootstrap from the legacy coordinator console
- `legacy/` - Files for the legacy coordinator console
- `legacy-coordinator-console.html` - Entry file for the legacy coordinator console
- `legacy-overlord-console.html` - Entry file for the legacy overlord console
- `lib/` - A place where some overrides to the react-table stylus files live, this is outside of the normal SCSS build system.
- `old-console/` - Files for the legacy overlord console 
- `pages/` - The files for the older legacy coordinator console
- `public/` - The compiled destination of the file powering this console
- `script/` - Some helper bash scripts for running this console
- `src/` - This directory (together with `lib`) constitutes all the source code for this console 


## List of non SQL data reading APIs used

```
GET /status
GET /druid/indexer/v1/supervisor?full
GET /druid/indexer/v1/workers
GET /druid/coordinator/v1/loadqueue?simple
GET /druid/coordinator/v1/config
GET /druid/coordinator/v1/metadata/datasources?includeDisabled
GET /druid/coordinator/v1/rules
GET /druid/coordinator/v1/config/compaction
GET /druid/coordinator/v1/tiers
```
