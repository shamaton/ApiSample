-- CREATE DATABASE game_shard_* CHARACTER SET utf8;
-- GRANT ALL PRIVILEGES ON `game_shard_*`.* TO 'game'@'localhost';

DROP TABLE IF EXISTS user;
CREATE TABLE `user` (
  id int(11) NOT NULL,
  name varchar(255),
  score int(11) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

BEGIN;
INSERT INTO user VALUES (1, "aaa", 100);
INSERT INTO user VALUES (2, "bbb", 70);
INSERT INTO user VALUES (3, "ccc", 50);
COMMIT;