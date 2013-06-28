-- Table structure for table `config`
--


DROP TABLE IF EXISTS prod_config;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE prod_config (
          name varchar(255) NOT NULL,
          payload bytea NOT NULL,
          PRIMARY KEY (name)
);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `rules`
--
DROP TABLE IF EXISTS prod_rules;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE prod_rules (
          id varchar(255) NOT NULL,
          dataSource varchar(255) NOT NULL,
          version text NOT NULL,
          payload text NOT NULL,
          PRIMARY KEY (id)
);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `segments`
--

DROP TABLE IF EXISTS prod_segments;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE prod_segments (
          id varchar(255) NOT NULL,
          dataSource varchar(255) NOT NULL,
          created_date text NOT NULL,
          start text NOT NULL,
          "end" text NOT NULL,
          partitioned SMALLINT NOT NULL,
          version text NOT NULL,
          used boolean NOT NULL,
          payload text NOT NULL,
          PRIMARY KEY (id)
);

