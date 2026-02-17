-- Test fixture: items table with multi-value insert

DROP TABLE IF EXISTS `items`;
CREATE TABLE `items` (
  `id` bigint(20) NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `items` VALUES (1,'Item One'),(2,'Item Two'),(3,'Item Three');
