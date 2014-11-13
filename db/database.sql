--
-- TAXONOMY
--
USE `taxonomy`;

DROP TABLE IF EXISTS `taxon`;
CREATE TABLE `taxon` (
	`id` INT UNSIGNED AUTO_INCREMENT,
	`parent_id` INT UNSIGNED DEFAULT NULL,
	`valid_name` VARCHAR(512) NOT NULL, -- Current valid name
	`rank` INT NOT NULL, -- See constants.py
	`comments` VARCHAR(65535) DEFAULT NULL,
	`data` TEXT DEFAULT NULL, -- Arbitrary data in JSON form
	`age` INT DEFAULT 0 NOT NULL, -- Age class (e, h, extant)
	`base_name_id` INT UNSIGNED DEFAULT NULL, -- Name that this taxon is based on
	`is_page_root` BOOL DEFAULT FALSE,
	PRIMARY KEY(`id`),
	INDEX(`valid_name`),
	INDEX(`base_name_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `name`;
CREATE TABLE `name` (
	`id` INT UNSIGNED AUTO_INCREMENT,
	`taxon_id` INT UNSIGNED NOT NULL, -- Name of valid taxon this belongs to
	`group` INT NOT NULL, -- Species, genus, family, or higher
	`status` INT NOT NULL, -- Valid, synonym, or species inquirenda
	`original_name` VARCHAR(512) DEFAULT NULL,
	`root_name` VARCHAR(512) NOT NULL,
	`authority` VARCHAR(1024) DEFAULT NULL,
	`year` VARCHAR(255) DEFAULT NULL,
	`page_described` VARCHAR(255) DEFAULT NULL,
	`original_citation` VARCHAR(512) DEFAULT NULL,
	`verbatim_type` VARCHAR(1024) DEFAULT NULL,
	`verbatim_citation` VARCHAR(1024) DEFAULT NULL,
	`type_id` INT UNSIGNED DEFAULT NULL, -- ID of type genus/species for family, genus group
	`nomenclature_comments` VARCHAR(65535) DEFAULT NULL,
	`taxonomy_comments` VARCHAR(65535) DEFAULT NULL,
	`other_comments` VARCHAR(65535) DEFAULT NULL,
	`data` TEXT DEFAULT NULL, -- Arbitrary data in JSON form
	PRIMARY KEY(`id`),
	INDEX(`original_name`),
	INDEX(`root_name`),
	INDEX(`taxon_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;
