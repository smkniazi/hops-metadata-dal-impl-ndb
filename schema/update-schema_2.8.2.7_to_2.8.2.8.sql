
CREATE TABLE `s3_metadata_store` (
  `bucket` VARCHAR(100) NOT NULL,
  `parent` VARCHAR(2655) NOT NULL,
  `child` VARCHAR(255) NOT NULL,
  `is_deleted` tinyint DEFAULT 0,
  `block_size` BIGINT NOT NULL,
  `file_length` BIGINT NOT NULL,
  `mod_time` BIGINT NOT NULL,
  `is_dir` tinyint DEFAULT 0,
  `table_created` BIGINT,
  `table_version` BIGINT,
  PRIMARY KEY (`bucket`, `parent`, `child`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs PARTITION BY KEY(parent);