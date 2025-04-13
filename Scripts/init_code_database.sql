Use master;
go

if exists ( select 1 from sys.database where name='Datawarehouse')
Begin 
  alter DATABASE Datawarehouse set singler_user with rollback immediate;
  drop DATABASE Datawarehouse;
end;
go

Create DATAEBASE Datawarehouse;
go

use Datawarehouse;
go

create schema bronze;
go 
create schema siler;
go
create schema gold;
go
