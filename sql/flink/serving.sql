-- Rendered into .local/flink/serving.sql by Rake (the catalog needs credentials).

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.dynamic-table-options.enabled' = 'true';
SET 'pipeline.name' = 'pcl-serving';

CREATE CATALOG pcl WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://polaris:8181/api/catalog',
    'warehouse' = '__PCL_ICEBERG_WAREHOUSE__',
    'rest.auth.type' = 'oauth2',
    'credential' = '__PCL_POLARIS_CLIENT_ID__:__PCL_POLARIS_CLIENT_SECRET__',
    'oauth2-server-uri' = 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    'scope' = 'PRINCIPAL_ROLE:ALL',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.region' = '__PCL_MINIO_REGION__',
    's3.access-key-id' = '__PCL_MINIO_ROOT_USER__',
    's3.secret-access-key' = '__PCL_MINIO_ROOT_PASSWORD__',
    'cache-enabled' = 'false'
);

USE CATALOG pcl;

-- Incremental append scan: raw is append-only, so the job follows the log.
INSERT INTO serving.orders_current /*+ OPTIONS('upsert-enabled'='true') */
SELECT order_id,
       customer_id,
       status,
       total_amount,
       currency,
       operation = 'DELETE' AS is_deleted,
       lsn                  AS last_lsn,
       tx_id                AS last_tx_id,
       seq_no               AS last_seq_no,
       event_id             AS last_event_id,
       operation            AS last_operation,
       source_timestamp     AS last_source_timestamp,
       coalesce(
         try_cast(image['updated_at'] AS TIMESTAMP(6)),
         source_timestamp,
         cast(current_timestamp AS TIMESTAMP(6))
       )                    AS updated_at,
       batch_id
  FROM (
    SELECT operation,
           lsn,
           tx_id,
           seq_no,
           event_id,
           source_timestamp,
           batch_id,
           coalesce(after_image, before_image) AS image,
           try_cast(coalesce(after_image, before_image)['order_id'] AS BIGINT) AS order_id,
           try_cast(coalesce(after_image, before_image)['customer_id'] AS BIGINT) AS customer_id,
           coalesce(after_image, before_image)['status'] AS status,
           try_cast(coalesce(after_image, before_image)['total_amount'] AS DECIMAL(12,2)) AS total_amount,
           coalesce(after_image, before_image)['currency'] AS currency
      FROM `raw`.`order_cdc_events`
      /*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */
     WHERE table_name = 'orders'
  ) events
 WHERE order_id IS NOT NULL;
