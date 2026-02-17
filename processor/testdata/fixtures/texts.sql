-- Test fixture: texts table with special characters

DROP TABLE IF EXISTS `texts`;
CREATE TABLE `texts` (
  `id` bigint(20) NOT NULL,
  `content` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `texts` VALUES (1,'Hello "World" with \'quotes\' and ñ unicode');
