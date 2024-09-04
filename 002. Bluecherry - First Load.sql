
/*
USE ROLE DATA_ENG_TEST_RW_RL;
USE WAREHOUSE DATA_ENGINEER_TEST_WH;
USE DATABASE LANDING_TEST;
USE SCHEMA BLUECHERRY;
*/

/*
USE ROLE DATA_ENG_DEV_RW_RL;
USE WAREHOUSE DATA_ENGINEER_DEV_WH;
USE DATABASE LANDING_DEV;
USE SCHEMA BLUECHERRY;
*/

----------------------------------------------------------------------------------------------------------------------------------
------------- 1. ZZEUPCNR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZEUPCNR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzeupcnr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
-------------- 2. ZZXBGTBL--------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXBGTBL
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
            t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
            t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
            t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
            t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
            t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
            t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
            t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
            t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
            t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
            t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
            t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
            t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
            t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
            t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
            t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
            t.$251, t.$252, t.$253, t.$254, t.$255, t.$256, t.$257, t.$258, t.$259, t.$260,
            t.$261, t.$262, t.$263, t.$264, t.$265, t.$266, t.$267, t.$268, t.$269, t.$270,
            t.$271, t.$272, t.$273, t.$274, t.$275, t.$276,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxbgtbl_2.*')T) 
    ON_ERROR=CONTINUE;
    
----------------------------------------------------------------------------------------------------------------------------------
------------- 3. ZZXCNTYR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

 COPY INTO ZZXCNTYR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxcntyr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------- 4. ZZXCOLRR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXCOLRR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxcolrr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------- 5. ZZXCONTR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXCONTR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxcontr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------- 6. ZZXHSNOR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXHSNOR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
    METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
    METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
        CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, 'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxhsnor_2.*')T) 
    ON_ERROR=CONTINUE;
  
----------------------------------------------------------------------------------------------------------------------------------
------------- 7. ZZXINTSD---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXINTSD
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxintsd_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------- 8. ZZXPGRPR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXPGRPR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxpgrpr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------- 9. ZZXSCOLR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXSCOLR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
            t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
            t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
            t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
            t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
            t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
            t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
            t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
            t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
            t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
            t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
            t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
            t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
            t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
            t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
            t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
            t.$251, t.$252, t.$253, t.$254, t.$255, t.$256, t.$257, t.$258, t.$259, t.$260,
            t.$261, t.$262, t.$263, t.$264, t.$265, t.$266, t.$267, t.$268, t.$269, t.$270,
            t.$271, t.$272, t.$273, t.$274, t.$275, t.$276, t.$277, t.$278, t.$279, t.$280,
            t.$281, t.$282, t.$283, t.$284, t.$285, t.$286, t.$287, t.$288, t.$289, t.$290,
            t.$291, t.$292, t.$293, t.$294, t.$295, t.$296, t.$297, t.$298, t.$299, t.$300,
            t.$301, t.$302,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
    METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
    METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
        CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, 'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxscolr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------ 10. ZZXSTYLR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXSTYLR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
            t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
            t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
            t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
            t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
            t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
            t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
            t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
            t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
            t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
            t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
            t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
            t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
            t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
            t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
    METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
    METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
        CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, 'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxstylr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
----------- 11.  ZZXCUSTR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXCUSTR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxcustr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------ 12. ZZXSTORR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXSTORR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxstorr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------ 13. ZZOORDRH---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZOORDRH
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
            t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
            t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
            t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
            t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
            t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
            t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
            t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
            t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
            t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
            t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
            t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
            t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
            t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
            t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
            t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
            t.$251, t.$252, t.$253, t.$254, t.$255, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzoordrh_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------ 14. ZZOORDRD---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZOORDRD
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
            t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
            t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
            t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
            t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
            t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
            t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
            t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
            t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
            t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
            t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
            t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
            t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
            t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
            t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
            t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
            t.$251, t.$252, t.$253, t.$254, t.$255, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzoordrd_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------ 15. ZVEUPCNR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZVEUPCNR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zveupcnr_20240821.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------ 16. ZZXBUCKT---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXBUCKT
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51,  
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxbuckt_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
-------------17. ZZXSIZER---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXSIZER
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxsizer_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
------------ 18. ZZOSHPRD---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZOSHPRD
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
            t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
            t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
            t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
            t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
            t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
            t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
            t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
            t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
            t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
            t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
            t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
            t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
            t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
            t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
            t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
            t.$251, t.$252, t.$253, t.$254, t.$255, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzoshprd_2.*')T) 
    ON_ERROR=CONTINUE ;

----------------------------------------------------------------------------------------------------------------------------------
------------ 19. ZZOSHPRH---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

    COPY INTO ZZOSHPRH
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
            t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
            t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
            t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
            t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
            t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
            t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
            t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
            t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
            t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
            t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
            t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
            t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
            t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
            t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
            t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
            t.$251, t.$252, t.$253, t.$254, t.$255, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzoshprh_2.*')T) 
    ON_ERROR=CONTINUE;
    
----------------------------------------------------------------------------------------------------------------------------------
------------- 20. ZZXDISCR---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

    COPY INTO ZZXDISCR
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxdiscr_2.*')T) 
    ON_ERROR=CONTINUE;

----------------------------------------------------------------------------------------------------------------------------------
-------------- 21. ZZNRETND---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZNRETND
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
            t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
            t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
            t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
            t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
            t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
            t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
            t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
            t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
            t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
            t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
            t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
            t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
            t.$201, t.$202, t.$203, t.$204, t.$205, t.$206,
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zznretnd_20240822.*')T) 
    ON_ERROR=CONTINUE 
    ;

----------------------------------------------------------------------------------------------------------------------------------
------------- 22. ZZNRETNH---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZNRETNH
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
            t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
            t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
            t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
            t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
            t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
            t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, 
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zznretnh_20240822.*')T) 
    ON_ERROR=CONTINUE 
    ;

----------------------------------------------------------------------------------------------------------------------------------
------------- 23. ZZXCOMMU---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------

COPY INTO ZZXCOMMU
    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
            t.$11, t.$12,  
            METADATA$FILENAME AS METADATA_FILENAME, 
            METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
            METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
            METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, 
            NULL AS ETL_ROW_UPDATED_TS, 
            'BLUECHERRY' AS ETL_SOURCE_SYSTEM_CD
    FROM @STAGE_S3_BLUECHERRY_PROD/(file_format => 'STAGE_CSV_FORMAT', PATTERN => 'year.*/dbo.zzxcommu_2.*')T) 
    ON_ERROR=CONTINUE 
    ;
