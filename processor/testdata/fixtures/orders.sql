-- Test fixture: orders table (non-sensitive data)

DROP TABLE IF EXISTS `orders`;
CREATE TABLE `orders` (
  `id` bigint(20) NOT NULL,
  `user_id` bigint(20) NOT NULL,
  `total` decimal(10,2) NOT NULL,
  `status` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `orders` VALUES (100,1,99.99,'completed');
