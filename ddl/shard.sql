-- CREATE DATABASE game_shard_* CHARACTER SET utf8;
-- GRANT ALL PRIVILEGES ON `game_shard_*`.* TO 'game'@'localhost';

DROP TABLE IF EXISTS user;
CREATE TABLE `user` (
  id bigint(20) unsigned NOT NULL,
  name varchar(255),
  score int(11) unsigned NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS user_test_log;
CREATE TABLE `user_test_log` (
  id bigint(20) unsigned NOT NULL,
  user_id bigint(20) unsigned NOT NULL,
  test_value int(11) unsigned NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY i1 (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

BEGIN;
INSERT INTO game_shard_1.user(id, name, score) VALUES (1, "aaa", 100) ON DUPLICATE KEY UPDATE id = id;
INSERT INTO game_shard_2.user(id, name, score) VALUES (2, "bbb", 70) ON DUPLICATE KEY UPDATE id = id;
INSERT INTO game_shard_1.user(id, name, score) VALUES (3, "ccc", 50) ON DUPLICATE KEY UPDATE id = id;
COMMIT;