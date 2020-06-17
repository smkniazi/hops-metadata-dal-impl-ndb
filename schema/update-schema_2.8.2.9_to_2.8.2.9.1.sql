ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_quota_cloud` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_used_cloud` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_quota_update` ADD COLUMN `typespace_delta_cloud` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_block_infos` ADD COLUMN `cloud_bucket_id` SMALLINT(4) NOT NULL DEFAULT -1;

ALTER TABLE `hdfs_invalidated_blocks` ADD COLUMN `cloud_bucket_id` SMALLINT(4) NOT NULL DEFAULT -1;

CREATE TABLE `hdfs_provided_block_cached_location` (`block_id` bigint NOT NULL, `storage_id` int DEFAULT NULL, PRIMARY KEY (`block_id`));

CREATE TABLE `hdfs_provided_block_report_tasks` ( `id` bigint(20) NOT NULL AUTO_INCREMENT, `start_index` bigint(20) NOT NULL, `end_index` bigint(20) NOT NULL, PRIMARY KEY (`id`));
