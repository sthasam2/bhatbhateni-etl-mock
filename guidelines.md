# schema naming

DBNAME_SCHEMANAME stg, temp, target;

Steps

- file -> csv `Download(csv)` -- manually

-- script

- file_stage store file `PUT`
- staging schema load - copy into table from file_stage
- truncate temp table
- insert into temp table
- upsert into target table
