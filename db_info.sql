CREATE DATABASE bhatbhateni_etl;

USE DATABASE Bhatbhateni_etl;

SELECT
    schema transactions;

SHOW TABLES;

-- created_on	                    name	        database_name	schema_name	    kind	comment	cluster_by	rows	bytes	owner	        retention_time	automatic_clustering	change_tracking	search_optimization	search_optimization_progress	search_optimization_bytes	is_external	owner_role_type
-- 2023-04-19 08:55:25.110 -0700	CATEGORY	    BHATBHATENI	    TRANSACTIONS	TABLE			            3	    1024	ACCOUNTADMIN    	1	        OFF	                    OFF	            OFF	                null	                        null	                    N	        ROLE
-- 2023-04-19 08:55:22.589 -0700	COUNTRY	        BHATBHATENI	    TRANSACTIONS	TABLE			            2	    1024	ACCOUNTADMIN    	1	        OFF	                    OFF	            OFF	                null	                        null	                    N	        ROLE
-- 2023-04-19 08:55:27.927 -0700	CUSTOMER	    BHATBHATENI	    TRANSACTIONS	TABLE			            6	    2560	ACCOUNTADMIN    	1	        OFF	                    OFF	            OFF	                null	                        null	                    N	        ROLE
-- 2023-04-19 08:55:27.025 -0700	PRODUCT	        BHATBHATENI	    TRANSACTIONS	TABLE			            18	    1536	ACCOUNTADMIN    	1	        OFF	                    OFF	            OFF	                null	                        null	                    N	        ROLE
-- 2023-04-19 08:55:23.365 -0700	REGION	        BHATBHATENI	    TRANSACTIONS	TABLE			            4	    1536	ACCOUNTADMIN    	1	        OFF	                    OFF	            OFF	                null	                        null	                    N	        ROLE
-- 2023-04-19 08:55:28.753 -0700	SALES	        BHATBHATENI	    TRANSACTIONS	TABLE			            100	    4608	ACCOUNTADMIN    	1	        OFF	                    OFF	            OFF	                null	                        null	                    N	        ROLE
-- 2023-04-19 08:55:24.347 -0700	STORE	        BHATBHATENI	    TRANSACTIONS	TABLE			            4	    1536	ACCOUNTADMIN    	1	        OFF	                    OFF	            OFF	                null	                        null	                    N	        ROLE
-- 2023-04-19 08:55:26.087 -0700	SUBCATEGORY	    BHATBHATENI	    TRANSACTIONS	TABLE			            9	    1536	ACCOUNTADMIN    	1	        OFF	                    OFF	            OFF	                null	                        null	                    N	        ROLE
SHOW warehouses;

-- name	state	type	size	min_cluster_count	max_cluster_count	started_clusters	running	queued	is_default	is_current	auto_suspend	auto_resume	available	provisioning	quiescing	other	created_on	resumed_on	updated_on	owner	comment	enable_query_acceleration	query_acceleration_max_scale_factor	resource_monitor	actives	pendings	failed	suspended	uuid	scaling_policy
-- COMPUTE_WH	SUSPENDED	STANDARD	X-Small	1	1	0	0	0	Y	Y	600	true					2023-04-17 22:17:18.416 -0700	2023-04-25 00:51:56.699 -0700	2023-04-25 00:51:56.699 -0700	ACCOUNTADMIN		false	8	null	0	0	0	1	1574864644	STANDARD


