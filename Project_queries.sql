#creating logistic_stg_db database
create database logistic_stg_db;

#creating logistic_dwh_db database
create database logistic_dwh_db;

use logistic_stg_db;

###########################################################################################
					#creating staging database tables
###########################################################################################

#-- 1)creating 'Employee_Details' table 
create table Employee_Details(Emp_ID varchar(30),Emp_NAME varchar(30),Emp_BRANCH varchar(30),Emp_DESIGNATION varchar(40),Emp_ADDR varchar(100),Emp_CONT_NO varchar(30));

#-- 2)membership_table
create table Membership(M_ID varchar(30) ,START_DATE text,END_DATE text);

#--  3)Status table
create table Status(CURRENT_ST varchar(30),SENT_DATE text,DELIVERY_DATE text,SH_ID varchar(30));

#-- 4)Customer_Table
create table Customer(Cust_ID varchar(30),Cust_NAME varchar(30),Cust_EMAIL_ID varchar(50)
,Cust_CONT_NO varchar(30),Cust_ADDR varchar(100),Cust_TYPE varchar(30),Membership_M_ID varchar(30));

#-- 5)Shipment_Details
create table Shipment_Details(SD_ID varchar(30),SD_CONTENT varchar(40),SD_DOMAIN varchar(30),SD_TYPE varchar(30)
,SD_WEIGHT varchar(30),SD_CHARGES varchar(30),SD_ADDR varchar(100),DS_ADDR varchar(100),Customer_Cust_ID varchar(30));

#-- 6)Payment_Details Table
create table Payment_Details(PAYMENT_ID VARCHAR(40),AMOUNT INT,PAYMENT_STATUS VARCHAR(30)
,PAYMENT_DATE text,PAYMENT_MODE VARCHAR(30),Shipment_SH_ID VARCHAR(30),Shipment_Cleint_C_ID varchar(30));

#-- 7)Employee table
create table Employee_Manage_Shipment(Employee_E_ID varchar(30),Shipment_SH_ID varchar(30),Status_SH_ID varchar(30));
###########################################################################################
					#Loading data into staging database
###########################################################################################
#creating staging tables;
LOAD DATA INFILE 'C:\Users\RHR\Desktop\github_projects\git_projects\sql_logistic\datasets\Customer.csv' INTO TABLE Customer 
COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 LINES;
LOAD DATA INFILE 'C:\Users\RHR\Desktop\github_projects\git_projects\sql_logistic\datasets\Employee_Details.csv' INTO TABLE Employee_Details 
COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 LINES;
LOAD DATA INFILE 'C:\Users\RHR\Desktop\github_projects\git_projects\sql_logistic\datasets\employee_manages_shipment.csv' INTO TABLE employee_manages_shipment 
COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 LINES;
LOAD DATA INFILE 'C:\Users\RHR\Desktop\github_projects\git_projects\sql_logistic\datasets\Membership.csv' INTO TABLE Membership
COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 LINES;
LOAD DATA INFILE 'C:\Users\RHR\Desktop\github_projects\git_projects\sql_logistic\datasets\Payment_Details.csv' INTO TABLE Payment_Details 
COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 LINES;
LOAD DATA INFILE 'C:\Users\RHR\Desktop\github_projects\git_projects\sql_logistic\datasets\Shipment_Details.csv' INTO TABLE Shipment_Details 
COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 LINES;
LOAD DATA INFILE 'C:\Users\RHR\Desktop\github_projects\git_projects\sql_logistic\datasets\Status.csv' INTO TABLE Status 
COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 LINES;
###########################################################################################
					#creating tables for duplicate records
###########################################################################################
#1)dup_Employee_details
#Creating table for Employee_details table duplicate records(dup_Employee_details)
CREATE TABLE dup_Employee_details like Employee_details;

#2)dup_membership
#Creating table for membership table duplicate records(dup_membership)
CREATE TABLE dup_membership like membership;

#3)dup_Customer
#Creating table for Customer table duplicate records(dup_Customer)
CREATE TABLE dup_Customer like Customer;

#4)dup_payment_details
#Creating table for payment_details table duplicate records(dup_payment_details)
CREATE TABLE dup_payment_details like payment_details;

#5)dup_shipment_details
#1)Creating table for shipment_details table duplicate records(dup_shipment_details)
CREATE TABLE dup_shipment_details like shipment_details;

#6)dup_status
#Creating table for status table duplicate records(dup_status)
CREATE TABLE dup_status like status;

###########################################################################################
					#Loading duplicate records into duplicate tables
###########################################################################################
#1)Employee_details
#inserting duplicate records into dup_Employee_details table
INSERT INTO dup_Employee_details select E_ID,E_NAME,E_DESIGNATION,E_ADDR,E_BRANCH,E_CONT_NO from 
(select *,row_number() over (partition  by E_ID order by E_ID asc) as rno from Employee_details) a
where rno>1;

#2)membership
#inserting duplicate records into dup_membership table
INSERT INTO dup_membership select M_ID,Start_date,End_date from 
(select *,row_number() over (partition  by M_ID order by M_ID asc) as rno from membership) a
where rno>1;

#3)Customer
#inserting duplicate records into dup_Customer table
INSERT INTO dup_Customer select C_ID,M_ID,C_NAME,C_EMAIL_ID,C_TYPE,C_ADDR,C_CONT_NO from 
(select *,row_number() over (partition  by C_ID order by C_ID asc) as rno from Customer) a
where rno>1;

#4)dup_payment_details
#inserting duplicate records into dup_payment_details table
INSERT INTO dup_payment_details
select Payment_ID,C_ID,SH_ID,AMOUNT,Payment_Status,Payment_Mode,Payment_Date from 
(select *,row_number() over(partition  by SH_ID order by SH_ID asc) as rno from payment_details) a
where rno>1;

#5)dup_Shipment_details
#inserting duplicate records into dup_shipment_details table
INSERT INTO dup_shipment_details
select SH_ID,C_ID,SH_CONTENT,SH_DOMAIN,SER_TYPE,SH_WEIGHT,SH_CHARGES,SR_ADDR,DS_ADDR from 
(select *,row_number() over(partition  by SH_ID order by SH_ID asc) as rno from shipment_details) a
where rno>1;

#6)dup_Status
#inserting duplicate records into dup_status table
INSERT INTO dup_status select SH_ID,Current_Status,Sent_date,Delivery_date from 
(select *,row_number() over (partition  by SH_ID order by SH_ID asc) as rno from status) a 
where rno>1;

###########################################################################################
					#Creating data warehouse database tables with Relationship
###########################################################################################

use logistic_dwh_db;
#-- 1)creating 'Employee_Details' table 
create table Dim_Employee_Details(Emp_ID int,Emp_NAME varchar(30),Emp_BRANCH varchar(15)
,Emp_DESIGNATION varchar(40),Emp_ADDR varchar(100),Emp_CONT_NO varchar(10));
 
#-- 2)membership_table
create table dim_Membership(M_ID int,START_DATE text,END_DATE text);

#--  3)Status table
create table dim_Status(CURRENT_ST varchar(15),SENT_DATE text,DELIVERY_DATE text,SH_ID varchar(6));

#-- 4)Customer_Table
create table dim_Customer(Cust_ID int,Cust_NAME varchar(30),Cust_EMAIL_ID varchar(50)
,Cust_CONT_NO varchar(10),Cust_ADDR varchar(100),Cust_TYPE varchar(30),Membership_M_ID int);

#-- 5)Shipment_Details
create table dim_Shipment_Details(SD_ID varchar(6),SD_CONTENT varchar(40),SD_DOMAIN varchar(15),SD_TYPE varchar(10)
,SD_WEIGHT varchar(10),SD_CHARGES INT(10),SD_ADDR varchar(100),DS_ADDR varchar(100),Customer_Cust_ID int(4));

#-- 6)Payment_Details Table
create table dim_Payment_Details(PAYMENT_ID VARCHAR(40),AMOUNT INT,PAYMENT_STATUS VARCHAR(10)
,PAYMENT_DATE text,PAYMENT_MODE VARCHAR(25),Shipment_SH_ID VARCHAR(6),Shipment_Cleint_C_ID INT(4));

#-- 7)Employee table
create table Employee_Manage_Shipment(Employee_E_ID int,Shipment_SH_ID varchar(6),Status_SH_ID varchar(6));

###########################################################################################
					#Loading data into data warehouse database tables 
###########################################################################################
use logistic_dwh_db;

#loading date from staging_db to dim and dimentional and fact tables
#from Employee_Details to dim_Employee_Details table 
INSERT INTO logistic_dwh_db.dim_employee_details select convert(E_ID, int(5))
,E_NAME,convert(E_BRANCH,varchar(15)),E_DESIGNATION,E_ADDR,convert(E_CONT_NO,varchar(10)) 
from logistic_stg_db.employee_details;

#from Status to dim_Status table 
INSERT INTO logistic_dwh_db.dim_Status(CURRENT_ST,SENT_DATE,DELIVERY_DATE,SH_ID)
select convert(Current_Status, varchar(15)),SENT_DATE,DELIVERY_DATE,convert(SH_ID,varchar(6))
 from logistic_stg_db.Status;
 
#from Membership to dim_Membership table 
INSERT INTO logistic_dwh_db.dim_membership select convert(M_ID, int),START_DATE,END_DATE
 from logistic_stg_db.membership;

#from customer to dim_customer table 
INSERT INTO logistic_dwh_db.dim_customer(Cust_ID
,Membership_M_ID,Cust_NAME, Cust_EMAIL_ID, Cust_TYPE, Cust_ADDR, Cust_CONT_NO)
select convert(C_ID,int(4)), convert(M_ID,int), C_NAME, C_EMAIL_ID, C_TYPE, C_ADDR, convert(C_CONT_NO,varchar(10))
 from logistic_stg_db.customer;

#from Shipment_details to dim_shipment_details table 
INSERT INTO logistic_dwh_db.dim_shipment_details(SD_ID, Customer_Cust_ID , SD_CONTENT
, SD_DOMAIN, SD_TYPE, SD_WEIGHT, SD_CHARGES, SD_ADDR, DS_ADDR) select convert(SD_ID,varchar(6))
, convert(C_ID,int(4)), SH_CONTENT,convert(SH_DOMAIN,varchar(15)),convert(SER_TYPE,varchar(15)), SH_CONTENT
,convert(SH_WEIGHT,varchar(10)),convert(SH_CHARGES,int(10)),SR_ADDR , DS_ADDR from logistic_stg_db.shipment_details;

#from payment_details to dim_payment_details table 
INSERT INTO logistic_dwh_db.dim_payment_details (PAYMENT_ID, Shipment_Cleint_C_ID, Shipment_SH_ID
, AMOUNT, PAYMENT_STATUS, PAYMENT_MODE, PAYMENT_DATE) select Payment_ID,convert(C_ID,int(4)), SH_ID, AMOUNT
, convert(Payment_Status,varchar(10)),convert(Payment_Mode,varchar(25))
, Payment_Date from logistic_stg_db.payment_details;

#from employee_manages_shipment table to fact_employee_manages_shipment table 
INSERT INTO logistic_dwh_db.fact_Employee_Manage_Shipment( Employee_E_ID,Shipment_SH_ID,Status_SH_ID)
select convert(Employee_E_ID,int(5)),convert(Shipment_SH_ID,varchar(6)),convert(Status_SH_ID,varchar(6)) 
from logistic_stg_db.employee_manages_shipment;



########################################################################################
################################## THE END #############################################



