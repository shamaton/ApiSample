BEGIN;

-- CREATE DATABASE game_master CHARACTER SET utf8;
-- GRANT ALL PRIVILEGES ON `game_master`.* TO 'game'@'localhost';

DROP TABLE IF EXISTS `user_shard`;
CREATE TABLE `user_shard` (
  id bigint(20) unsigned NOT NULL,
  shard_id tinyint(3) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='user sharding table';


DROP TABLE IF EXISTS `db_table_conf`;
CREATE TABLE `db_table_conf` (
  id bigint(20) unsigned NOT NULL,
  table_name varchar(255) NOT NULL,
  use_type tinyint(3) unsigned NOT NULL COMMENT '1:master 2:shard',
  shard_type tinyint(3) unsigned NOT NULL COMMENT '0:none 1:user 2:group(TBD...)',
  PRIMARY KEY (`id`),
  UNIQUE KEY (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='table conf master';

-- sequence table
DROP TABLE IF EXISTS `seq_user_test_log`;
CREATE TABLE `seq_user_test_log` (
  id bigint(20) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='sequence table for user_test_log';
INSERT INTO seq_user_test_log VALUES (0);

INSERT INTO user_shard VALUES (1, 1);
INSERT INTO user_shard VALUES (2, 2);
INSERT INTO user_shard VALUES (3, 1);

INSERT INTO db_table_conf VALUES (1, "db_table_conf", 1, 0);
INSERT INTO db_table_conf VALUES (2, "user_shard", 1, 0);
INSERT INTO db_table_conf VALUES (3, "user", 2, 1);
INSERT INTO db_table_conf VALUES (4, "user_test_log", 2, 1);

COMMIT;