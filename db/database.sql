--
-- TAXONOMY
--
USE `taxonomy`;

DROP TABLE IF EXISTS `taxon`;
CREATE TABLE `taxon` (
	`id` INT UNSIGNED AUTO_INCREMENT,
	`parent` INT UNSIGNED NOT NULL,
	`valid_name` VARCHAR(512) NOT NULL, -- Current valid name
	`rank` INT NOT NULL, -- See constants.py
	`comments` VARCHAR(65535) DEFAULT NULL,
	`data` TEXT DEFAULT NULL, -- Arbitrary data in JSON form
	PRIMARY KEY(`id`),
	INDEX(`valid_name`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `name`;
CREATE TABLE `name` (
	`id` INT UNSIGNED AUTO_INCREMENT,
	`taxon_id` INT UNSIGNED NOT NULL, -- Name of valid taxon this belongs to
	`group` INT NOT NULL, -- Species, genus, family, or higher
	`status` INT NOT NULL, -- Valid, synonym, or species inquirenda
	`original_name` VARCHAR(512) DEFAULT NULL,
	`base_name` VARCHAR(512) NOT NULL,
	`authority` VARCHAR(1024) DEFAULT NULL,
	`year` VARCHAR(255) DEFAULT NULL,
	`page_described` VARCHAR(255) DEFAULT NULL,
	`original_citation` VARCHAR(512) DEFAULT NULL,
	`type_id` INT UNSIGNED DEFAULT NULL, -- ID of type genus/species for family, genus group
	`nomenclature_comments` VARCHAR(65535) DEFAULT NULL,
	`taxonomy_comments` VARCHAR(65535) DEFAULT NULL,
	`other_comments` VARCHAR(65535) DEFAULT NULL,
	`data` TEXT DEFAULT NULL, -- Arbitrary data in JSON form
	PRIMARY KEY(`id`),
	INDEX(`original_name`),
	INDEX(`base_name`),
	INDEX(`taxon_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;
