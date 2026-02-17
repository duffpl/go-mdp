-- Test fixture: audit_log table for skip testing

DROP TABLE IF EXISTS `audit_log`;
CREATE TABLE `audit_log` (
  `id` bigint(20) NOT NULL,
  `action` varchar(100) NOT NULL,
  `user_id` bigint(20) NOT NULL,
  `details` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `audit_log` VALUES (1,'login',42,'User logged in from 192.168.1.1');
INSERT INTO `audit_log` VALUES (2,'update',42,'Changed email to new@example.com');
