CREATE OR REPLACE DATABASE BANKS_DETAIL;

USE BANKS_DETAIL;


CREATE OR REPLACE TABLE DISTRICT(
District_Code INT PRIMARY KEY,
District_Name VARCHAR(100),
Region VARCHAR(100),
No_of_inhabitants INT,
No_of_municipalties_with_inhabitants_less_499 INT,
No_of_municipalities_with_inhabitants_500_btw_1999 INT,
No_of_municipalities_with_inhabitants_2000_btw_9999 INT,
No_of_municipalities_with_inhabitants_greater_10000 INT,
No_of_cities INT,
Ratio_of_urban_inhabitants DECIMAL(4,1),
Average_salary INT,
Unemployment_rate_2018 DECIMAL(3,2),
Unemployment_rate_2019 DECIMAL(3,2),
No_of_entrepreneurs_per_1000_inhabitants INT,
No_committed_crime_2018 INT,
No_committed_crime_2019 INT
);


CREATE OR REPLACE TABLE ACCOUNT(
Account_Id INT PRIMARY KEY,
District_Id INT,
Frequency VARCHAR(50),
DATE INT,
Account_Type VARCHAR(50),
FOREIGN KEY (District_Id) REFERENCES DISTRICT(District_Code)
);

CREATE OR REPLACE TABLE CLIENT(
Client_Id INT PRIMARY KEY,
SEX VARCHAR(10),
BIRTH_DATE DATE,
District_Id INT,
FOREIGN KEY (District_Id) REFERENCES DISTRICT(District_Code)
);

CREATE OR REPLACE TABLE DISPOSITION(
Disp_Id INT PRIMARY KEY,
Client_Id INT,
Account_Id INT,
TYPE VARCHAR(10),
FOREIGN KEY (Account_Id) REFERENCES ACCOUNT(Account_Id),
FOREIGN KEY (Client_Id) REFERENCES CLIENT(Client_Id)
);


CREATE OR REPLACE TABLE CARD(
Card_Id INT,
Disp_Id INT,
TYPE VARCHAR(50),
ISSUED INT,
FOREIGN KEY (Disp_iD) REFERENCES DISPOSITION(Disp_Id)
);


CREATE OR REPLACE TABLE ORDER_LIST(
Order_Id INT PRIMARY KEY,
Account_Id INT,
Bank_to VARCHAR(100),
Account_to INT,
Amount DECIMAL(6, 1),
FOREIGN KEY (Account_Id) REFERENCES ACCOUNT(Account_Id)
);


CREATE OR REPLACE TABLE LOAN(
Loan_Id INT,
Account_Id INT,
Date INT,
Amount INT,
Duration INT,
Payments INT,
Status VARCHAR(10),
FOREIGN KEY (Account_Id) REFERENCES ACCOUNT(Account_Id)
);


CREATE OR REPLACE TABLE TRANSACTIONS(
Trans_Id INT,
Account_Id INT,
Date DATE,
TYPE VARCHAR(50),
Operation VARCHAR(50),
Amount INT,
Balance DECIMAL(7,1),
Purpose VARCHAR(100),
Bank VARCHAR(100),
Account_Partner_Id INT,
FOREIGN KEY (Account_Id) REFERENCES ACCOUNT(Account_Id)
);



---------------------------------------------------------------------------------------------


CREATE OR REPLACE STORAGE integration s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::441615131317:role/bankrole'
STORAGE_ALLOWED_LOCATIONS =('s3://czechbankdata/');

DESC integration s3_int;


CREATE OR REPLACE STAGE BANK
URL ='s3://czechbankdata'
--credentials=(aws_key_id='AKIAXQKR3H3PSG72XFMK'aws_secret_key='eKL6a6FjlQHic4s8Ne712Aelzg2ou4j6tNsVvFq5')
file_format = CSV
storage_integration = s3_int;

LIST @BANK;

SHOW STAGES;

-- CREATE SNOWPIPE THAT RECOGNISES CSV THAT ARE INGESTED FROM EXTERNAL STAGE AND COPIES THE DATA INTO EXISTING TABLE

-- The AUTO_INGEST=true parameter specifies to read 
-- event notifications sent from an S3 bucket to an SQS queue when new data is ready to load.


CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISTRICT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."DISTRICT" --yourdatabase -- your schema ---your table
FROM '@BANK/District/' --s3 bucket subfolde4r name
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ACCOUNT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."ACCOUNT"
FROM '@BANK/Account/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_TXNS AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."TRANSACTIONS"
FROM '@BANK/Trnx/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISP AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."DISPOSITION"
FROM '@BANK/disp/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CARD AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."CARD"
FROM '@BANK/Card/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ORDER_LIST AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."ORDER_LIST"
FROM '@BANK/Order/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_LOAN AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."LOAN"
FROM '@BANK/Loan/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CLIENT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."CLIENT"
FROM '@BANK/Client/'
FILE_FORMAT = CSV;

SHOW PIPES;

ALTER PIPE BANK_SNOWPIPE_DISTRICT refresh;

ALTER PIPE BANK_SNOWPIPE_ACCOUNT refresh;

ALTER PIPE BANK_SNOWPIPE_TXNS refresh;

ALTER PIPE BANK_SNOWPIPE_DISP refresh;

ALTER PIPE BANK_SNOWPIPE_CARD refresh;

ALTER PIPE BANK_SNOWPIPE_ORDER_LIST refresh;

ALTER PIPE BANK_SNOWPIPE_LOAN refresh;

ALTER PIPE BANK_SNOWPIPE_CLIENT refresh;

-----------------------------------------------------------------------------------------------------------------------------


-- File Formats

CREATE OR REPLACE FILE FORMAT BANKS_DETAIL.public.file_csv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    field_optionally_enclosed_by = 'none'
    skip_header = 1 ; 