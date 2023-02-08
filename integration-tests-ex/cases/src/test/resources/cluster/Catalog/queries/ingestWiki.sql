REPLACE INTO "testWiki" OVERWRITE ALL
SELECT
  TIME_PARSE("timestamp") AS "__time",
  namespace,
  page,
  channel,
  "user",
  countryName,
  CASE WHEN isRobot = 'true' THEN 1 ELSE 0 END AS isRobot,
  "added",
  "delta",
  CASE WHEN isNew = 'true' THEN 1 ELSE 0 END AS isNew,
  "deltaBucket",
  "deleted"
FROM TABLE(ext.sampleWiki(files => ?))
