/*
===========================================
Create database and schemas
===========================================
Scrit purpose:
  This scripts creates a new database named DataWarehouseMercedes after checking if it already exists.
  If the data base exists, it is dropped and recreated. Additionally, the scripts sets up three scheùas within the database : bronze, silver and gold.

Warning:
  Running the script will drop the entire DataWarehouseMercedes database if it exists.
  Ensure to have a backup before running it.
*/

use master;
go

if exists (select 1 from sys.databses where name = 'DataWarehouseMercedes')
begin
  alter databse DataWarehouseMercedes set single_user with rollback immediate;
  drop database DataWarehouseMercedes;
end;
go
create database DataWarehouseMercedes;
go
use DataWarehouseMercedes;
go
create schema bronze;
go
create schema silver;
go
create schema gold;
go
