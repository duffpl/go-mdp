-- Test fixture: contacts table for row variable testing

DROP TABLE IF EXISTS `contacts`;
CREATE TABLE `contacts` (
  `id` bigint(20) NOT NULL,
  `full_name` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `contacts` VALUES (42,'Jane Smith','jane@real.com');
