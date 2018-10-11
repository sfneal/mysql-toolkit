from mysql.toolkit.script.prepare import prepare_sql


stmnt = '''
CREATE TABLE `HOUSE INFO 2` (
  `PROJECT NUMBER` int(11) DEFAULT NULL,
  `HOUSE STYLE` int(11) DEFAULT NULL,
  `1ST FLOOR SQ FT` int(11) DEFAULT NULL,
  `2ND FLOOR SQ FT` int(11) DEFAULT NULL,
  `SQUARE FOOTAGE` int(11) DEFAULT NULL,
  `GENERAL SQUARE FOOTAGE` int(11) DEFAULT NULL,
  `LENGTH` int(11) DEFAULT NULL,
  `DEPTH` int(11) DEFAULT NULL,
  `FAMILY` int(11) DEFAULT NULL,
  `BOX WIDTH` int(11) DEFAULT NULL,
  `BOX DEPTH` int(11) DEFAULT NULL,
  `1ST FLOOR CEILING HEIGHT` int(11) DEFAULT NULL,
  `2ND FLOOR CEILING HEIGHT` int(11) DEFAULT NULL,
  `NUMBER OF FACADE PROJECTIONS` int(11) DEFAULT NULL,
  `PROJECTION LOCATION` int(11) DEFAULT NULL,
  `BASEMENT TYPE` int(11) DEFAULT NULL,
  `ROOMS` int(11) DEFAULT NULL,
  `BEDROOMS` int(11) DEFAULT NULL,
  `FULL BATHS` int(11) DEFAULT NULL,
  `1/2 BATHS` int(11) DEFAULT NULL,
  `GARAGE SIZE` int(11) DEFAULT NULL,
  `GARAGE LOCATION` int(11) DEFAULT NULL,
  `ROOF TYPE` int(11) DEFAULT NULL,
  `1ST FLOOR MASTER` int(11) DEFAULT NULL,
  `IN-LAW SUITE` int(11) DEFAULT NULL,
  `GOLF CART GARAGE` int(11) DEFAULT NULL,
  `BREEZEWAY` int(11) DEFAULT NULL,
  `FARMER'S PORCH` int(11) DEFAULT NULL,
  `MISC/COMMENTS` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `HOUSE INFO 2`
--

LOCK TABLES `HOUSE INFO 2` WRITE;
/*!40000 ALTER TABLE `HOUSE INFO 2` DISABLE KEYS */;
/*!40000 ALTER TABLE `HOUSE INFO 2` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `HomePlan`
--

DROP TABLE IF EXISTS `HomePlan`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `HomePlan` (
  `cObjectID` int(11) DEFAULT NULL,
  `creationDate` varchar(19) DEFAULT NULL,
  `planNumber` int(11) DEFAULT NULL,
  `projectID` varchar(4) DEFAULT NULL,
  `projectNumber` int(11) DEFAULT NULL,
  `priceCode` varchar(1) DEFAULT NULL,
  `footageFirst` int(11) DEFAULT NULL,
  `footageSecond` varchar(7) DEFAULT NULL,
  `footageOther` varchar(4) DEFAULT NULL,
  `footageTotal` int(11) DEFAULT NULL,
  `totalLength` int(11) DEFAULT NULL,
  `totalDepth` int(11) DEFAULT NULL,
  `boxLength` int(11) DEFAULT NULL,
  `boxDepth` int(11) DEFAULT NULL,
  `rooms` int(11) DEFAULT NULL,
  `bedrooms` int(11) DEFAULT NULL,
  `bathsFull` int(11) DEFAULT NULL,
  `bathsHalf` int(11) DEFAULT NULL,
  `familyRoomLocationID` int(11) DEFAULT NULL,
  `garageSize` int(11) DEFAULT NULL,
  `garageLocationID` int(11) DEFAULT NULL,
  `projectionCount` int(11) DEFAULT NULL,
  `projectionLocationID` int(11) DEFAULT NULL,
  `basementTypeID` int(11) DEFAULT NULL,
  `mainRoofTypeID` int(11) DEFAULT NULL,
  `firstFloorMaster` int(11) DEFAULT NULL,
  `inlawSuite` int(11) DEFAULT NULL,
  `golfCartGarage` int(11) DEFAULT NULL,
  `farmersPorch` int(11) DEFAULT NULL,
  `breezeway` int(11) DEFAULT NULL,
  `displayPhotosPublicly` int(11) DEFAULT NULL,
  `saleCertified` int(11) DEFAULT NULL,
  `comments` text,
  `firstFloorPlan_big` varchar(34) DEFAULT NULL,
  `secondFloorPlan_big` varchar(35) DEFAULT NULL,
  `frontElevation_big` varchar(35) DEFAULT NULL,
  `photo_big` varchar(34) DEFAULT NULL,
  `frontElevation_small` varchar(38) DEFAULT NULL,
  `photo_small` varchar(36) DEFAULT NULL,
  `sketch_small` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;
'''
# print(stmnt)
# print('------------------------------------------------------------------------')
# print(prepare_sql(stmnt))


prepare_sql(stmnt)
