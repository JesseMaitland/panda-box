SELECT tablename  AS table_name,
       tableowner AS table_owner,
       tablespace AS schema_name
FROM pg_tables;
