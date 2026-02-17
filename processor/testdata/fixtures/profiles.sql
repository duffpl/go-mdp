-- Test fixture: profiles table with empty value

DROP TABLE IF EXISTS `profiles`;
CREATE TABLE `profiles` (
  `id` bigint(20) NOT NULL,
  `bio` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `profiles` VALUES (1,'');
