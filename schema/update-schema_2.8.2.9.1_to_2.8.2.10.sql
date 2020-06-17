ALTER TABLE hdfs_file_provenance_xattrs_buffer PARTITION BY KEY(inode_id);
CREATE TABLE `hdfs_cloud_buckets` ( `id` SMALLINT(4) NOT NULL AUTO_INCREMENT, `name` varchar(256) UNIQUE, PRIMARY KEY (`id`))
