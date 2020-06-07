CREATE EXTERNAL TABLE Employees ( 
Name STRING,
JobTitles STRING,
Department STRING, 
F_or_P STRING, 
S_or_H STRING, 
TYPICALYHOURS Decimal, 
AnnualSalary Decimal, 
HourlyRate Decimal
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
STORED AS TEXTFILE 
LOCATION '/user/cloudera/input/' 
TBLPROPERTIES ("skip.header.line.count"="1");

--Q1
SELECT Department, MAX(AnnualSalary) AS MaxSalary 
FROM Employees 
GROUP BY Department;

--Q2 
SELECT Department, Count(Department) AS NumberOfEmployees 
FROM Employees 
GROUP BY Department;

--Q3
SELECT Department, 
Count(CASE WHEN s_or_h =='Hourly' THEN Department ELSE NULL END) AS HourlyEmployees , 
Count(CASE WHEN s_or_h =='Salary' THEN Department ELSE NULL END) AS SalaryEmployees
FROM Employees 
GROUP BY Department;