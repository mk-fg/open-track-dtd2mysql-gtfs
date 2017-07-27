-- MySQL dump 10.16  Distrib 10.1.24-MariaDB, for Linux (x86_64)
--
-- Host: 10.0.10.2    Database: cif
-- ------------------------------------------------------
-- Server version	10.1.24-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `alias`
--

DROP TABLE IF EXISTS `alias`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alias` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `station_name` char(26) NOT NULL,
  `station_alias` char(26) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `alias_key` (`station_name`)
) ENGINE=InnoDB AUTO_INCREMENT=296 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `association`
--

DROP TABLE IF EXISTS `association`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `association` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `base_uid` char(6) NOT NULL,
  `assoc_uid` char(6) NOT NULL,
  `start_date` date NOT NULL,
  `end_date` date NOT NULL,
  `monday` tinyint(1) unsigned NOT NULL,
  `tuesday` tinyint(1) unsigned NOT NULL,
  `wednesday` tinyint(1) unsigned NOT NULL,
  `thursday` tinyint(1) unsigned NOT NULL,
  `friday` tinyint(1) unsigned NOT NULL,
  `saturday` tinyint(1) unsigned NOT NULL,
  `sunday` tinyint(1) unsigned NOT NULL,
  `assoc_cat` char(2) DEFAULT NULL,
  `assoc_date_ind` char(1) DEFAULT NULL,
  `assoc_location` char(7) NOT NULL,
  `base_location_suffix` char(1) NOT NULL,
  `assoc_location_suffix` char(1) NOT NULL,
  `association_type` char(1) DEFAULT NULL,
  `stp_indicator` char(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `base_uid` (`base_uid`),
  KEY `assoc_uid` (`assoc_uid`),
  KEY `assoc_location` (`assoc_location`),
  KEY `start_date` (`start_date`),
  KEY `end_date` (`end_date`)
) ENGINE=InnoDB AUTO_INCREMENT=2201 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fixed_link`
--

DROP TABLE IF EXISTS `fixed_link`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fixed_link` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `mode` varchar(10) NOT NULL,
  `origin` char(3) NOT NULL,
  `destination` char(3) NOT NULL,
  `duration` smallint(3) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `fixed_link_key` (`mode`,`origin`,`destination`)
) ENGINE=InnoDB AUTO_INCREMENT=983 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `physical_station`
--

DROP TABLE IF EXISTS `physical_station`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `physical_station` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `station_name` char(26) NOT NULL,
  `cate_interchange_status` tinyint(1) unsigned DEFAULT NULL,
  `tiploc_code` char(7) NOT NULL,
  `crs_reference_code` char(3) DEFAULT NULL,
  `crs_code` char(3) DEFAULT NULL,
  `minimum_change_time` tinyint(2) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `physical_station_key` (`tiploc_code`)
) ENGINE=InnoDB AUTO_INCREMENT=3148 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `schedule`
--

DROP TABLE IF EXISTS `schedule`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedule` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `train_uid` char(6) NOT NULL,
  `runs_from` date NOT NULL,
  `runs_to` date NOT NULL,
  `monday` tinyint(1) unsigned NOT NULL,
  `tuesday` tinyint(1) unsigned NOT NULL,
  `wednesday` tinyint(1) unsigned NOT NULL,
  `thursday` tinyint(1) unsigned NOT NULL,
  `friday` tinyint(1) unsigned NOT NULL,
  `saturday` tinyint(1) unsigned NOT NULL,
  `sunday` tinyint(1) unsigned NOT NULL,
  `bank_holiday_running` tinyint(1) unsigned NOT NULL,
  `train_status` char(1) DEFAULT NULL,
  `train_category` char(2) DEFAULT NULL,
  `train_identity` char(4) DEFAULT NULL,
  `headcode` char(4) DEFAULT NULL,
  `course_indicator` char(1) NOT NULL,
  `profit_center` char(8) DEFAULT NULL,
  `business_sector` char(1) DEFAULT NULL,
  `power_type` char(3) DEFAULT NULL,
  `timing_load` char(4) DEFAULT NULL,
  `speed` char(3) DEFAULT NULL,
  `operating_chars` char(6) DEFAULT NULL,
  `train_class` char(1) DEFAULT NULL,
  `sleepers` char(1) DEFAULT NULL,
  `reservations` char(1) DEFAULT NULL,
  `connect_indicator` char(1) DEFAULT NULL,
  `catering_code` char(4) DEFAULT NULL,
  `service_branding` char(4) DEFAULT NULL,
  `stp_indicator` char(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `schedule_key` (`train_uid`,`runs_from`,`stp_indicator`),
  KEY `runs_from` (`runs_from`)
) ENGINE=InnoDB AUTO_INCREMENT=244974 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `schedule_extra`
--

DROP TABLE IF EXISTS `schedule_extra`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schedule_extra` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `schedule` int(11) unsigned NOT NULL,
  `traction_class` char(4) DEFAULT NULL,
  `uic_code` char(5) DEFAULT NULL,
  `atoc_code` char(2) NOT NULL,
  `applicable_timetable_code` char(1) NOT NULL,
  `retail_train_id` char(8) NOT NULL,
  `source` char(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `schedule` (`schedule`)
) ENGINE=InnoDB AUTO_INCREMENT=215304 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stop_time`
--

DROP TABLE IF EXISTS `stop_time`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stop_time` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `schedule` int(11) unsigned NOT NULL,
  `location` char(8) NOT NULL,
  `scheduled_arrival_time` time DEFAULT NULL,
  `scheduled_departure_time` time DEFAULT NULL,
  `scheduled_pass_time` time DEFAULT NULL,
  `public_arrival_time` time DEFAULT NULL,
  `public_departure_time` time DEFAULT NULL,
  `platform` char(3) DEFAULT NULL,
  `line` char(3) DEFAULT NULL,
  `path` char(3) DEFAULT NULL,
  `activity` char(12) DEFAULT NULL,
  `engineering_allowance` char(2) DEFAULT NULL,
  `pathing_allowance` char(2) DEFAULT NULL,
  `performance_allowance` char(2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `stop_time_key` (`schedule`,`location`,`public_departure_time`)
) ENGINE=InnoDB AUTO_INCREMENT=3776452 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tiploc`
--

DROP TABLE IF EXISTS `tiploc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tiploc` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `tiploc_code` char(7) NOT NULL,
  `capitals` char(2) NOT NULL,
  `nalco` char(6) NOT NULL,
  `nlc_check_character` char(1) NOT NULL,
  `tps_description` char(26) NOT NULL,
  `stanox` char(5) NOT NULL,
  `po_mcp_code` smallint(4) unsigned NOT NULL,
  `crs_code` char(3) DEFAULT NULL,
  `description` char(16) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tiploc_key` (`tiploc_code`)
) ENGINE=InnoDB AUTO_INCREMENT=10997 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `z_schedule`
--

DROP TABLE IF EXISTS `z_schedule`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `z_schedule` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `train_uid` char(6) NOT NULL,
  `runs_from` date NOT NULL,
  `runs_to` date NOT NULL,
  `monday` tinyint(1) unsigned NOT NULL,
  `tuesday` tinyint(1) unsigned NOT NULL,
  `wednesday` tinyint(1) unsigned NOT NULL,
  `thursday` tinyint(1) unsigned NOT NULL,
  `friday` tinyint(1) unsigned NOT NULL,
  `saturday` tinyint(1) unsigned NOT NULL,
  `sunday` tinyint(1) unsigned NOT NULL,
  `bank_holiday_running` tinyint(1) unsigned NOT NULL,
  `train_status` char(1) DEFAULT NULL,
  `train_category` char(2) DEFAULT NULL,
  `train_identity` char(4) DEFAULT NULL,
  `headcode` char(4) DEFAULT NULL,
  `course_indicator` char(1) DEFAULT NULL,
  `profit_center` char(8) DEFAULT NULL,
  `business_sector` char(1) DEFAULT NULL,
  `power_type` char(3) DEFAULT NULL,
  `timing_load` char(4) DEFAULT NULL,
  `speed` char(3) DEFAULT NULL,
  `operating_chars` char(6) DEFAULT NULL,
  `train_class` char(1) DEFAULT NULL,
  `sleepers` char(1) DEFAULT NULL,
  `reservations` char(1) DEFAULT NULL,
  `connect_indicator` char(1) DEFAULT NULL,
  `catering_code` char(4) DEFAULT NULL,
  `service_branding` char(4) DEFAULT NULL,
  `stp_indicator` char(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `z_schedule_key` (`train_uid`,`runs_from`,`stp_indicator`),
  KEY `runs_from` (`runs_from`)
) ENGINE=InnoDB AUTO_INCREMENT=12475 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `z_stop_time`
--

DROP TABLE IF EXISTS `z_stop_time`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `z_stop_time` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `z_schedule` int(11) unsigned NOT NULL,
  `location` char(3) NOT NULL,
  `scheduled_arrival_time` time DEFAULT NULL,
  `scheduled_departure_time` time DEFAULT NULL,
  `scheduled_pass_time` time DEFAULT NULL,
  `public_arrival_time` time DEFAULT NULL,
  `public_departure_time` time DEFAULT NULL,
  `platform` char(3) DEFAULT NULL,
  `line` char(3) DEFAULT NULL,
  `path` char(3) DEFAULT NULL,
  `activity` char(12) DEFAULT NULL,
  `engineering_allowance` char(2) DEFAULT NULL,
  `pathing_allowance` char(2) DEFAULT NULL,
  `performance_allowance` char(2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `z_stop_time_key` (`z_schedule`,`location`,`public_departure_time`)
) ENGINE=InnoDB AUTO_INCREMENT=38354 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `additional_fixed_link`
--

DROP TABLE IF EXISTS `additional_fixed_link`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `additional_fixed_link` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `mode` varchar(10) NOT NULL,
  `origin` char(3) NOT NULL,
  `destination` char(3) NOT NULL,
  `duration` smallint(3) unsigned NOT NULL,
  `start_time` time DEFAULT NULL,
  `end_time` time DEFAULT NULL,
  `start_date` date NOT NULL,
  `end_date` date NOT NULL,
  `monday` tinyint(1) unsigned NOT NULL,
  `tuesday` tinyint(1) unsigned NOT NULL,
  `wednesday` tinyint(1) unsigned NOT NULL,
  `thursday` tinyint(1) unsigned NOT NULL,
  `friday` tinyint(1) unsigned NOT NULL,
  `saturday` tinyint(1) unsigned NOT NULL,
  `sunday` tinyint(1) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-06-22  8:58:46
