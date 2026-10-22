# API to Raw Table

## Purpose

Polls the Transport Victoria Open Data API for unplanned road disruptions and streams each JSON response into a Snowflake RAW table using Snowpipe Streaming.

## Components

- **Get API data** (InvokeHTTP): Sends a GET request to `#{api_url}` every 60 seconds, passing the API key in the `KeyId` header
- **Specify JSON structure** (JoltTransformJSON): Wraps the full API response under a `PAYLOAD` field so it lands in a single column
- **Insert data into Snowflake** (PutSnowpipeStreaming): Streams records into `#{snowflake_database}.#{snowflake_schema}.#{snowflake_table}` using `SNOWFLAKE_MANAGED` authentication
- **JsonTreeReader**: Record reader used by PutSnowpipeStreaming (inferred schema)
- **Funnel**: Collects failure, retry and non-response relationships

## Required NARs

- `org.apache.nifi:nifi-standard-nar` (InvokeHTTP)
- `org.apache.nifi:nifi-jolt-nar` (JoltTransformJSON)
- `org.apache.nifi:nifi-record-serialization-services-nar` (JsonTreeReader)
- `com.snowflake.openflow.runtime:runtime-snowpipe-processors-nar` (PutSnowpipeStreaming)

All are included in Snowflake Openflow runtimes.

## Parameters

Parameter context: **REST API Source**

| Name | Sensitive | Default | Description |
|------|-----------|---------|-------------|
| `api_url` | No | Transport Victoria unplanned disruptions endpoint | API endpoint to poll |
| `KeyId` | Yes | _(none)_ | Transport Victoria Open Data API key. Supply via a Snowflake secret, never in the flow JSON |
| `snowflake_database` | No | `OPENFLOW_DB` | Target database |
| `snowflake_schema` | No | `RAW` | Target schema |
| `snowflake_table` | No | `TRANSPORT_VIC_UNPLANNED_ROAD_DISRUPTIONS` | Target table |
| `snowflake_role` | No | `OPENFLOW_INGESTION_POC_DEV_INGEST` | Role used by Snowpipe Streaming |

## Configuration

- The runtime needs egress to `api.opendata.transport.vic.gov.au:443` (network rule + external access integration)
- The target table must exist with a `PAYLOAD` column (VARIANT), and `snowflake_role` must be able to insert into it
- Set `KeyId` from a Snowflake secret exposed through the Snowflake Parameter Provider

## Expected Behavior

Once started, the flow calls the API every 60 seconds and inserts one row per response into the target table. Failed or retried requests are routed to the funnel and do not reach Snowflake.
