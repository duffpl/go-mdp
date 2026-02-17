-- Test fixture: members table with multiple rows

DROP TABLE IF EXISTS `members`;
CREATE TABLE `members` (
  `id` bigint(20) NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `members` VALUES (1,'Alice');
INSERT INTO `members` VALUES (2,'Bob');
INSERT INTO `members` VALUES (3,'Charlie');
