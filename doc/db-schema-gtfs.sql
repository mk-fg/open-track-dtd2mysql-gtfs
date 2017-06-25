

-- --------------------
-- Notes:
--  - trips.txt is missing bikes_allowed field.
--  - All tables are missing db consistency constraints (e.g. foreign keys).


-- MySQL dump 10.16  Distrib 10.1.24-MariaDB, for Linux (x86_64)
--
-- Host: 10.0.10.2    Database: gtfs
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
-- Table structure for table `agency`
--

DROP TABLE IF EXISTS `agency`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `agency` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `agency_id` varchar(100) DEFAULT NULL,
  `agency_name` varchar(255) NOT NULL,
  `agency_url` varchar(255) NOT NULL,
  `agency_timezone` varchar(100) NOT NULL,
  `agency_lang` varchar(100) DEFAULT NULL,
  `agency_phone` varchar(100) DEFAULT NULL,
  `agency_fare_url` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `calendar`
--

DROP TABLE IF EXISTS `calendar`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `calendar` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `service_id` int(12) unsigned NOT NULL,
  `monday` tinyint(1) NOT NULL,
  `tuesday` tinyint(1) NOT NULL,
  `wednesday` tinyint(1) NOT NULL,
  `thursday` tinyint(1) NOT NULL,
  `friday` tinyint(1) NOT NULL,
  `saturday` tinyint(1) NOT NULL,
  `sunday` tinyint(1) NOT NULL,
  `start_date` date NOT NULL,
  `end_date` date NOT NULL,
  PRIMARY KEY (`id`),
  KEY `service_id` (`service_id`),
  KEY `start_date` (`start_date`),
  KEY `end_date` (`end_date`),
  KEY `monday` (`monday`),
  KEY `tuesday` (`tuesday`),
  KEY `wednesday` (`wednesday`),
  KEY `thursday` (`thursday`),
  KEY `friday` (`friday`),
  KEY `saturday` (`saturday`),
  KEY `sunday` (`sunday`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `calendar_dates`
--

DROP TABLE IF EXISTS `calendar_dates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `calendar_dates` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `service_id` varchar(20) NOT NULL,
  `date` date NOT NULL,
  `exception_type` tinyint(2) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `service_id` (`service_id`),
  KEY `exception_type` (`exception_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fare_attributes`
--

DROP TABLE IF EXISTS `fare_attributes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fare_attributes` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `fare_id` varchar(100) DEFAULT NULL,
  `price` varchar(50) NOT NULL,
  `currency_type` varchar(50) NOT NULL,
  `payment_method` tinyint(1) NOT NULL,
  `transfers` tinyint(1) NOT NULL,
  `transfer_duration` varchar(10) DEFAULT NULL,
  `exception_type` tinyint(2) NOT NULL,
  `agency_id` int(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fare_id` (`fare_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fare_rules`
--

DROP TABLE IF EXISTS `fare_rules`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fare_rules` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `fare_id` varchar(100) DEFAULT NULL,
  `route_id` varchar(100) DEFAULT NULL,
  `origin_id` varchar(100) DEFAULT NULL,
  `destination_id` varchar(100) DEFAULT NULL,
  `contains_id` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fare_id` (`fare_id`),
  KEY `route_id` (`route_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `feed_info`
--

DROP TABLE IF EXISTS `feed_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `feed_info` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `feed_publisher_name` varchar(100) DEFAULT NULL,
  `feed_publisher_url` varchar(255) NOT NULL,
  `feed_lang` varchar(255) NOT NULL,
  `feed_start_date` date NOT NULL,
  `feed_end_date` date DEFAULT NULL,
  `feed_version` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `frequencies`
--

DROP TABLE IF EXISTS `frequencies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `frequencies` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `trip_id` int(12) unsigned NOT NULL,
  `start_time` date NOT NULL,
  `end_time` date NOT NULL,
  `headway_secs` varchar(100) NOT NULL,
  `exact_times` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `trip_id` (`trip_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `routes`
--

DROP TABLE IF EXISTS `routes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `routes` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `route_id` varchar(100) DEFAULT NULL,
  `agency_id` varchar(50) DEFAULT NULL,
  `route_short_name` varchar(50) NOT NULL,
  `route_long_name` varchar(255) NOT NULL,
  `route_type` varchar(2) NOT NULL,
  `route_text_color` varchar(255) DEFAULT NULL,
  `route_color` varchar(255) DEFAULT NULL,
  `route_url` varchar(255) DEFAULT NULL,
  `route_desc` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `agency_id` (`agency_id`),
  KEY `route_type` (`route_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `shapes`
--

DROP TABLE IF EXISTS `shapes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `shapes` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `shape_id` varchar(100) NOT NULL,
  `shape_pt_lat` decimal(8,6) NOT NULL,
  `shape_pt_lon` decimal(8,6) NOT NULL,
  `shape_pt_sequence` tinyint(3) NOT NULL,
  `shape_dist_traveled` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `shape_id` (`shape_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stop_times`
--

DROP TABLE IF EXISTS `stop_times`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stop_times` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `trip_id` int(12) unsigned NOT NULL,
  `arrival_time` time DEFAULT NULL,
  `departure_time` time DEFAULT NULL,
  `stop_id` char(3) NOT NULL,
  `stop_sequence` tinyint(1) unsigned NOT NULL,
  `stop_headsign` varchar(50) DEFAULT NULL,
  `pickup_type` tinyint(1) unsigned DEFAULT NULL,
  `drop_off_type` tinyint(1) unsigned DEFAULT NULL,
  `shape_dist_traveled` varchar(50) DEFAULT NULL,
  `timepoint` tinyint(1) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `trip_id` (`trip_id`),
  KEY `arrival_time` (`arrival_time`),
  KEY `departure_time` (`departure_time`),
  KEY `stop_id` (`stop_id`),
  KEY `stop_sequence` (`stop_sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stops`
--

DROP TABLE IF EXISTS `stops`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stops` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `stop_id` char(3) DEFAULT NULL,
  `stop_code` varchar(50) DEFAULT NULL,
  `stop_name` varchar(255) NOT NULL,
  `stop_desc` varchar(255) DEFAULT NULL,
  `stop_lat` decimal(10,6) DEFAULT NULL,
  `stop_lon` decimal(10,6) DEFAULT NULL,
  `zone_id` varchar(255) DEFAULT NULL,
  `stop_url` varchar(255) DEFAULT NULL,
  `location_type` varchar(2) DEFAULT NULL,
  `parent_station` varchar(100) DEFAULT NULL,
  `stop_timezone` varchar(50) DEFAULT NULL,
  `wheelchair_boarding` tinyint(1) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `stop_id` (`stop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `transfers`
--

DROP TABLE IF EXISTS `transfers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `transfers` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `from_stop_id` char(3) NOT NULL,
  `to_stop_id` char(3) NOT NULL,
  `transfer_type` tinyint(1) unsigned NOT NULL,
  `min_transfer_time` int(8) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `trips`
--

DROP TABLE IF EXISTS `trips`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `trips` (
  `id` int(12) unsigned NOT NULL AUTO_INCREMENT,
  `route_id` varchar(100) NOT NULL,
  `service_id` int(12) unsigned NOT NULL,
  `trip_id` int(12) unsigned NOT NULL,
  `trip_headsign` varchar(255) DEFAULT NULL,
  `trip_short_name` varchar(255) DEFAULT NULL,
  `direction_id` tinyint(1) unsigned DEFAULT NULL,
  `block_id` varchar(11) DEFAULT NULL,
  `shape_id` varchar(11) DEFAULT NULL,
  `wheelchair_accessible` tinyint(1) unsigned DEFAULT NULL,
  `bikes_allowed` tinyint(1) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `trip` (`trip_headsign`(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-06-22  8:58:39
