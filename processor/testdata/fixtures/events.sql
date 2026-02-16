-- Test fixture: events table with JSON metadata
DROP TABLE IF EXISTS `events`;
CREATE TABLE `events` (
  `id` bigint(20) NOT NULL,
  `event_type` varchar(50) NOT NULL,
  `metadata` longtext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `events` VALUES (1,'login','{\"user\":{\"firstName\":\"John\",\"lastName\":\"Doe\",\"email\":\"john@real.com\"},\"timestamp\":\"2024-01-15\"}');
