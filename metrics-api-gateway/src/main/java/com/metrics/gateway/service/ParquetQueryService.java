package com.metrics.gateway.service;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Service for listing and (in production) querying Parquet files stored in MinIO.
 *
 * <p>Note: Full Parquet query capability (scanning file contents for aggregations or
 * filtered row reads) would require Apache Arrow's Java SDK or embedding DuckDB as an
 * in-process query engine. This implementation provides file listing and metadata access
 * only, which is sufficient for audit, data discovery, and export use cases.
 */
@Service
public class ParquetQueryService {

    private static final Logger log = LoggerFactory.getLogger(ParquetQueryService.class);

    private final MinioClient minioClient;

    @Value("${minio.bucket}")
    private String defaultBucket;

    public ParquetQueryService(MinioClient minioClient) {
        this.minioClient = minioClient;
    }

    /**
     * Lists Parquet file object names under the given prefix in the specified bucket.
     *
     * @param bucket the MinIO bucket name
     * @param prefix the object key prefix to filter by (e.g. "aggregated/production/2024-01-15")
     * @return list of object names (keys) matching the prefix
     */
    public List<String> listRecentFiles(String bucket, String prefix) {
        String targetBucket = (bucket != null && !bucket.isBlank()) ? bucket : defaultBucket;
        log.info("Listing Parquet files in bucket={} prefix={}", targetBucket, prefix);

        List<String> objectNames = new ArrayList<>();
        try {
            ListObjectsArgs.Builder argsBuilder = ListObjectsArgs.builder()
                    .bucket(targetBucket)
                    .recursive(true);

            if (prefix != null && !prefix.isBlank()) {
                argsBuilder.prefix(prefix);
            }

            Iterable<Result<Item>> results = minioClient.listObjects(argsBuilder.build());
            for (Result<Item> result : results) {
                Item item = result.get();
                String objectName = item.objectName();
                if (objectName.endsWith(".parquet") || objectName.endsWith(".snappy.parquet")) {
                    objectNames.add(objectName);
                    log.debug("Found Parquet file: {}", objectName);
                }
            }
        } catch (Exception e) {
            log.error("Failed to list objects in bucket={} prefix={}: {}",
                    targetBucket, prefix, e.getMessage(), e);
            throw new RuntimeException("Failed to list Parquet files from MinIO", e);
        }

        log.info("Found {} Parquet files in bucket={} prefix={}", objectNames.size(), targetBucket, prefix);
        return objectNames;
    }

    /**
     * Lists Parquet files in the default bucket under the given prefix.
     *
     * @param prefix the object key prefix to filter by
     * @return list of matching object names
     */
    public List<String> listRecentFiles(String prefix) {
        return listRecentFiles(defaultBucket, prefix);
    }
}
