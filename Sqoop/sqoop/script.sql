-- sql commands
mysql -u root -p
--Enter password as cloudera
show databases;
create database cs523;
use cs523;
create table stocks (id int not null primary key, symbol varchar(20),qoute_date varchar(20),open_price decimal(10,2),hight_price decimal(10,2), low_price decimal(10,2));

describe stocks;

insert into stocks values (1, "AAPL", "2009-01-02" , 85.88, 91.04, 85.16), (2, "AAPL", "2008-01-02" , 199.27, 200.26, 192.55), (3, "AAPL", "2007-01-03" , 86.29, 86.58, 81.9);

select * from stocks; 

quit;

--Sqoop Commands

sqoop import --connect jdbc:mysql://quickstart.cloudera/cs523 
--username root -password cloudera 
--table stocks 
--columns "id, symbol,open_price" 
--fields-terminated-by "\t" 
--target-dir=/user/cloudera/sqoopImportOutputStocks
