-- Test fixture: events table with JSON arrays
DROP TABLE IF EXISTS `events`;
CREATE TABLE `events` (
  `id` bigint(20) NOT NULL,
  `event_type` varchar(50) NOT NULL,
  `metadata` longtext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `events` VALUES (1,'tagged','{\"tags\":[{\"key\":\"source\",\"value\":\"web\"},{\"key\":\"browser\",\"value\":\"chrome\"}]}');
