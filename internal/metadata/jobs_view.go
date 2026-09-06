package metadata

// JobsHistoryViewName is the storage name of the view that exposes job
// history in the BigQuery INFORMATION_SCHEMA.JOBS shape.
//
// The view lives in the metadata namespace (an unqualified, root-level
// catalog object, like the `jobs` table itself). User queries never
// reference it directly: the content layer rewrites region-qualified
// `INFORMATION_SCHEMA.JOBS` references into a projection over it.
// Being root-level keeps it invisible to INFORMATION_SCHEMA.SCHEMATA /
// TABLES / COLUMNS, whose synthesizers skip single-segment name paths.
const JobsHistoryViewName = "bq_emulator_information_schema_jobs"

// jobsHistoryViewDDL derives the INFORMATION_SCHEMA.JOBS compatible
// view from the metadata `jobs` table — the very same rows that back
// jobs.get / jobs.list. Because the view is computed live from that
// table, history rows can never duplicate or drift from the REST
// metadata: job insertion, failure, cancellation and deletion all
// mutate `jobs` inside one transaction, and both surfaces read it.
//
// Column names and types mirror BigQuery's region-scoped
// INFORMATION_SCHEMA.JOBS view. Fields the emulator does not track
// (parent_job_id, reservation, referenced_tables contents) are
// emitted as NULL / empty typed values so every row has the full
// column set.
var jobsHistoryViewDDL = `
CREATE VIEW IF NOT EXISTS ` + JobsHistoryViewName + ` AS
SELECT
  TIMESTAMP_SECONDS(CAST(JSON_VALUE(metadata, '$.statistics.creationTime') AS INT64)) AS creation_time,
  TIMESTAMP_SECONDS(CAST(JSON_VALUE(metadata, '$.statistics.startTime') AS INT64)) AS start_time,
  TIMESTAMP_SECONDS(CAST(JSON_VALUE(metadata, '$.statistics.endTime') AS INT64)) AS end_time,
  id AS job_id,
  projectID AS project_id,
  IFNULL(JSON_VALUE(metadata, '$.configuration.jobType'), 'QUERY') AS job_type,
  IFNULL(JSON_VALUE(metadata, '$.status.state'), 'DONE') AS state,
  IF(
    JSON_VALUE(metadata, '$.status.errorResult.reason') IS NULL
      AND JSON_VALUE(metadata, '$.status.errorResult.message') IS NULL,
    CAST(NULL AS STRUCT<reason STRING, location STRING, message STRING>),
    STRUCT(
      JSON_VALUE(metadata, '$.status.errorResult.reason') AS reason,
      JSON_VALUE(metadata, '$.status.errorResult.location') AS location,
      JSON_VALUE(metadata, '$.status.errorResult.message') AS message
    )
  ) AS error_result,
  JSON_VALUE(metadata, '$.configuration.query.query') AS query,
  IF(
    COALESCE(
      JSON_VALUE(metadata, '$.configuration.query.destinationTable.tableId'),
      JSON_VALUE(metadata, '$.configuration.load.destinationTable.tableId')
    ) IS NULL,
    CAST(NULL AS STRUCT<project_id STRING, dataset_id STRING, table_id STRING>),
    STRUCT(
      COALESCE(
        JSON_VALUE(metadata, '$.configuration.query.destinationTable.projectId'),
        JSON_VALUE(metadata, '$.configuration.load.destinationTable.projectId')
      ) AS project_id,
      COALESCE(
        JSON_VALUE(metadata, '$.configuration.query.destinationTable.datasetId'),
        JSON_VALUE(metadata, '$.configuration.load.destinationTable.datasetId')
      ) AS dataset_id,
      COALESCE(
        JSON_VALUE(metadata, '$.configuration.query.destinationTable.tableId'),
        JSON_VALUE(metadata, '$.configuration.load.destinationTable.tableId')
      ) AS table_id
    )
  ) AS destination_table,
  CAST(
    ARRAY<STRUCT<project_id STRING, dataset_id STRING, table_id STRING>>[]
    AS ARRAY<STRUCT<project_id STRING, dataset_id STRING, table_id STRING>>
  ) AS referenced_tables,
  IFNULL(CAST(JSON_VALUE(metadata, '$.statistics.totalBytesProcessed') AS INT64), 0) AS total_bytes_processed,
  IFNULL(CAST(JSON_VALUE(metadata, '$.statistics.query.totalBytesBilled') AS INT64), 0) AS total_bytes_billed,
  IFNULL(CAST(JSON_VALUE(metadata, '$.statistics.query.cacheHit') AS BOOL), FALSE) AS cache_hit,
  JSON_VALUE(metadata, '$.configuration.query.priority') AS priority,
  JSON_VALUE(metadata, '$.statistics.query.statementType') AS statement_type,
  JSON_VALUE(metadata, '$.userEmail') AS user_email,
  CAST(NULL AS STRING) AS parent_job_id,
  CAST(NULL AS STRING) AS reservation
FROM jobs
`
