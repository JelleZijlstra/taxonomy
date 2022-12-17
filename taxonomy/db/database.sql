--
-- TAXONOMY
--
USE `taxonomy`;

CREATE TABLE `taxon` (
    `id` INT UNSIGNED AUTO_INCREMENT,
    `parent_id` INT UNSIGNED DEFAULT NULL,
    `valid_name` VARCHAR(512) DEFAULT NULL, -- Current valid name
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

CREATE TABLE `name` (
    `id` INT UNSIGNED AUTO_INCREMENT,
    `taxon_id` INT UNSIGNED NOT NULL, -- Name of valid taxon this belongs to
    `group` INT NOT NULL, -- Species, genus, family, or higher
    `status` INT NOT NULL, -- Valid, synonym, or species inquirenda
    `original_name` VARCHAR(512) DEFAULT NULL,
    `corrected_original_name` VARCHAR(512) DEFAULT NULL,
    `root_name` VARCHAR(512) NOT NULL,
    `authority` VARCHAR(1024) DEFAULT NULL,
    `year` VARCHAR(255) DEFAULT NULL,
    `page_described` VARCHAR(255) DEFAULT NULL,
    `original_citation` VARCHAR(512) DEFAULT NULL,
    `original_citation_id` INT UNSIGNED DEFAULT NULL,
    `verbatim_type` VARCHAR(1024) DEFAULT NULL,
    `verbatim_citation` VARCHAR(1024) DEFAULT NULL,
    `type_id` INT UNSIGNED DEFAULT NULL, -- ID of type genus/species for family, genus group
    `nomenclature_comments` VARCHAR(65535) DEFAULT NULL,
    `taxonomy_comments` VARCHAR(65535) DEFAULT NULL,
    `other_comments` VARCHAR(65535) DEFAULT NULL,
    `data` TEXT DEFAULT NULL, -- Arbitrary data in JSON form
    `stem` VARCHAR(512) DEFAULT NULL,
    `gender` TINYINT UNSIGNED DEFAULT NULL,
    `definition` VARCHAR(1024) DEFAULT NULL,
    `type_locality_id` int(10) UNSIGNED DEFAULT NULL,
    `type_locality_description` MEDIUMTEXT,
    `type_specimen` varchar(1024) DEFAULT NULL,
    `nomenclature_status` INT NOT NULL DEFAULT 1, -- available or not
    `name_complex_id` INT UNSIGNED DEFAULT NULL,
    `species_name_complex_id` INT UNSIGNED DEFAULT NULL,
    `collection_id` integer default null,
    `type_description` varchar(65535) default null,
    `type_specimen_source` varchar(512) default null, -- unused
    `type_specimen_source_id` INT UNSIGNED DEFAULT NULL, -- unused
    `type_kind` integer default null, -- don't want it, drop this once I figure out the SQLite syntax
    `tags` varchar(65535) default null,
    `genus_type_kind` integer default null,
    `species_type_kind` integer default null,
    `type_tags` text default null,
    `citation_group` INT UNSIGNED DEFAULT NULL,
    `author_tags` text default null,
    `original_rank` integer default null,
    PRIMARY KEY(`id`),
    INDEX(`original_name`),
    INDEX(`root_name`),
    INDEX(`taxon_id`),
    INDEX(`type_locality_id`),
    INDEX(`corrected_original_name`),
    INDEX(`citation_group`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE `region` (
    `id` INT UNSIGNED AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    `comment` VARCHAR(65535) DEFAULT NULL,
    `parent_id` INT UNSIGNED,
    `kind` INT UNSIGNED,  # continent, country, subnational
    PRIMARY KEY(`id`),
    INDEX(`name`),
    UNIQUE KEY(`name`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE `period` (
    `id` INT UNSIGNED AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    `parent_id` INT UNSIGNED,
    `prev_id` INT UNSIGNED,
    `next_id` INT UNSIGNED, -- should remove
    `min_age` INT UNSIGNED,
    `max_age` INT UNSIGNED,
    `system` INT UNSIGNED,
    `rank` INT UNSIGNED,
    `comment` VARCHAR(65535) DEFAULT NULL,
    `min_period_id` INT UNSIGNED DEFAULT NULL,
    `max_period_id` INT UNSIGNED DEFAULT NULL,
    `region_id` INT UNSIGNED DEFAULT NULL,
    `deleted` BOOL DEFAULT FALSE,
    PRIMARY KEY(`id`),
    INDEX(`name`),
    UNIQUE KEY(`name`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE `stratigraphic_unit` (
    `id` INT UNSIGNED AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    `parent_id` INT UNSIGNED,
    `prev_id` INT UNSIGNED,
    `rank` INT UNSIGNED,
    `comment` VARCHAR(65535) DEFAULT NULL,
    `min_period_id` INT UNSIGNED DEFAULT NULL,
    `max_period_id` INT UNSIGNED DEFAULT NULL,
    `region_id` INT UNSIGNED DEFAULT NULL,
    `deleted` BOOL DEFAULT FALSE,
    PRIMARY KEY(`id`),
    INDEX(`name`),
    UNIQUE KEY(`name`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE `location` (
    `id` INT UNSIGNED AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    `min_period_id` INT UNSIGNED DEFAULT NULL,
    `max_period_id` INT UNSIGNED DEFAULT NULL,
    `min_age` INT UNSIGNED DEFAULT NULL,
    `max_age` INT UNSIGNED DEFAULT NULL,
    `region_id` INT UNSIGNED NOT NULL,
    `comment` VARCHAR(65535) DEFAULT NULL,
    `stratigraphic_unit_id` INT UNSIGNED DEFAULT NULL,
    `latitude` VARCHAR(255) DEFAULT NULL,
    `longitude` VARCHAR(255) DEFAULT NULL,
    `location_detail` text,
    `age_detail` text,
    `source` varchar(255) default null,
    `source_id` INT UNSIGNED DEFAULT NULL,
    `deleted` INT UNSIGNED,
    `tags` text default null,
    `parent_id` INT UNSIGNED DEFAULT NULL,
    PRIMARY KEY(`id`),
    INDEX(`name`),
    UNIQUE KEY(`name`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE `occurrence` (
    `id` INT UNSIGNED AUTO_INCREMENT,
    `taxon_id` INT UNSIGNED,
    `location_id` INT UNSIGNED,
    `comment` VARCHAR(65535) DEFAULT NULL,
    `source` VARCHAR(1023),
    `source_id` INT UNSIGNED DEFAULT NULL,
    `status` INT UNSIGNED,
    PRIMARY KEY(`id`),
    INDEX(`taxon_id`),
    INDEX(`location_id`),
    UNIQUE KEY(`taxon_id`, `location_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE `name_complex` (
    `id` integer primary key,
    `label` varchar(255),
    `stem` varchar(255) default NULL,
    `source_language` integer,
    `code_article` integer,
    `gender` integer,
    `comment` varchar(65535),
    `stem_remove` varchar(255) default NULL,
    `stem_add` varchar(255) default NULL
);

CREATE TABLE `name_ending` (
    `id` integer primary key,
    'name_complex_id' integer,
    `ending` varchar(255),
    `comment` varchar(65535)
);

CREATE TABLE `species_name_complex` (
    `id` integer primary key,
    `label` varchar(255),
    `stem` varchar(255) default NULL,
    `kind` integer,
    `comment` varchar(65535),
    `masculine_ending` varchar(255) default NULL,
    `feminine_ending` varchar(255) default NULL,
    `neuter_ending` varchar(255) default NULL
);

CREATE TABLE `species_name_ending` (
    `id` integer primary key,
    'name_complex_id' integer,
    `ending` varchar(255),
    `comment` varchar(65535),
    `full_name_only` integer default 0,
);

CREATE TABLE `collection` (
    `id` integer primary key,
    `label` varchar(255),
    `name` varchar(255),
    `location_id` integer,
    `comment` varchar(65535),
    `city` varchar(255) default null,
    `removed` integer default 0,
);

CREATE TABLE `name_comment` (
    `id` integer primary key,
    `name_id` integer not null,
    `kind` integer,
    `date` integer,
    `text` text,
    `source` varchar(512),
    `source_id` INT UNSIGNED DEFAULT NULL,
    `page` varchar(512)
);

CREATE TABLE `article` (
    `addmonth` integer NOT NULL,
    `addday` integer NOT NULL,
    `addyear` integer NOT NULL,
    `folder` varchar(255) DEFAULT NULL,
    `sfolder` varchar(255) DEFAULT NULL,
    `ssfolder` varchar(255) DEFAULT NULL,
    `sssfolder` varchar(255) DEFAULT NULL,
    `name` varchar(255) NOT NULL,
    `authors` varchar(255) DEFAULT NULL,
    `year` varchar(255) DEFAULT NULL,
    `title` varchar(2048) DEFAULT NULL,
    `journal` varchar(512) DEFAULT NULL,
    `series` varchar(255) DEFAULT NULL,
    `volume` varchar(255) DEFAULT NULL,
    `issue` varchar(255) DEFAULT NULL,
    `start_page` varchar(255) DEFAULT NULL,
    `end_page` varchar(255) DEFAULT NULL,
    `url` varchar(255) DEFAULT NULL,
    `doi` varchar(255) DEFAULT NULL,
    `type` integer DEFAULT NULL,
    `publisher` varchar(255) DEFAULT NULL,
    `location` varchar(255) DEFAULT NULL,
    `pages` varchar(255) DEFAULT NULL,
    `ids` varchar(1024) DEFAULT NULL,
    `bools` varchar(1024) DEFAULT NULL,
    `parent` varchar(255) DEFAULT NULL,
    `misc_data` varchar(4096) DEFAULT NULL,
    `path` varchar(255) DEFAULT NULL,
    `kind` integer DEFAULT NULL,
    `parent_id` INT UNSIGNED DEFAULT NULL,
    `tags` text default null,
    `citation_group_id` INT UNSIGNED DEFAULT NULL,
    `author_tags` text default null
    PRIMARY KEY (`name`),
    INDEX(`citation_group_id`)
);

CREATE TABLE `article_comment` (
    `id` integer primary key,
    `article_id` integer not null,
    `kind` integer,
    `date` integer,
    `text` text
);

CREATE TABLE `citation_group` (
    `id` integer primary key,
    `name` varchar(255) NOT NULL,
    `region_id` INTEGER DEFAULT NULL,
    `deleted` TINYINT DEFAULT 0,
    `type` integer NOT NULL DEFAULT 0,
    `target_id` integer default null,
    `tags` text default null,
    `archive` text default null,
    UNIQUE KEY(`name`)
);

CREATE TABLE `citation_group_pattern` (
    `id` integer primary key,
    `citation_group_id` int unsigned not null,
    `pattern` varchar(255) not null,
    unique key(`pattern`)
);

CREATE TABLE `person` (
  `id` integer  NOT NULL PRIMARY KEY AUTOINCREMENT,
  `family_name` varchar(255) NOT NULL,
  `given_names` varchar(255) DEFAULT NULL,
  `initials` varchar(255) DEFAULT NULL,
  `suffix` varchar(255) DEFAULT NULL,
  `tussenvoegsel` varchar(255) DEFAULT NULL,
  `birth` varchar(255) DEFAULT NULL,
  `death` varchar(255) DEFAULT NULL,
  `tags` text default null,
  `naming_convention` INTEGER NOT NULL DEFAULT 1,
  `type` INTEGER NOT NULL DEFAULT 1,
  `target_id` INTEGER DEFAULT NULL,
  `bio` TEXT DEFAULT NULL,
  `ol_id` VARCHAR(15) DEFAULT NULL
);
CREATE INDEX "idx_person_name" ON "person" (`family_name`);
CREATE INDEX "idx_ol_id" on "person" (`ol_id`);

CREATE TABLE `book` (
    `id` integer  NOT NULL PRIMARY KEY AUTOINCREMENT,
    `author_tags` text default null,
    `year` varchar(255) DEFAULT NULL,
    `title` varchar(2048) DEFAULT NULL,
    `subtitle` varchar(255) DEFAULT NULL,
    `pages` varchar(255) DEFAULT NULL,
    `isbn` varchar(255) DEFAULT NULL,
    `publisher` varchar(255) DEFAULT NULL,
    `tags` text default null,
    `citation_group_id` INT UNSIGNED DEFAULT NULL,
    `dewey` varchar(127) DEFAULT NULL,
    `loc` varchar(127) DEFAULT NULL,
    `data` text default null,
    `category` varchar(255) DEFAULT NULL
);

CREATE TABLE `specimen` (
    `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,
    `taxon_id` INT UNSIGNED NOT NULL,
    `region_id` INT UNSIGNED NOT NULL,
    `taxon_text` VARCHAR(255),
    `location_text` VARCHAR(255),
    `date` VARCHAR(255),
    `description` VARCHAR(2047),
    `link` VARCHAR(255) DEFAULT NULL
);

CREATE TABLE `specimen_comment` (
    `id` integer primary key,
    `specimen_id` integer not null,
    `date` integer,
    `text` text
);
CREATE INDEX "idx_specimen" ON "specimen_comment" (`specimen_id`);
