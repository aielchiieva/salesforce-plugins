# Salesforce Batch Source


Description
-----------
This source reads sObjects from Salesforce.
Examples of sObjects are opportunities, contacts, accounts, leads, any custom object, etc.

The data which should be read is specified using SOQL queries (Salesforce Object Query Language queries)

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Username:** Salesforce username.

**Password:** Salesforce password.

**Client Id:** Application Client Id. This is also called "Consumer Key" in Salesforce UI.
To create Client Id user needs to create a connected application in Salesforce first.

**Client Secret:** Application Client Secret. This is also called "Consumer Secret" in Salesforce UI.
To create Client Secret user needs to create a connected application in Salesforce first.

**Login Url:** Salesforce oauth2 login url.

**SOQL Query:** A SOQL query to fetch data into source.

Examples:

``SELECT Id, Name, BillingCity FROM Account``

``SELECT Id FROM Contact WHERE Name LIKE 'A%' AND MailingCity = 'California'``

**SObject Name:** Salesforce object name to read. If value is provided plugin will get all fields for this object from 
Salesforce and generate SOQL query (`select <FIELD_1, FIELD_2, ..., FIELD_N> from ${sObjectName}`). 
Ignored if SOQL query is provided. 

**Datetime Filter:** SObject query datetime filter applied to system field `LastModifiedDate` to allow incremental 
query. Filter will be added to SObject query as `WHERE LastModifiedDate > ${datetimeFilter}`. If value is not provided, 
it means all records to be read since the beginning of time. 
The filter can be one of two types: 

`SOQL Date Format` - string should be in Salesforce Date Formats. 

    +----------------------------------+---------------------------+---------------------------+
    |              Format              |       Format Syntax       |          Example          |
    +----------------------------------+---------------------------+---------------------------+
    | Date, time, and time zone offset | YYYY-MM-DDThh:mm:ss+hh:mm | 1999-01-01T23:01:01+01:00 |
    |                                  | YYYY-MM-DDThh:mm:ss-hh:mm | 1999-01-01T23:01:01-08:00 |
    |                                  | YYYY-MM-DDThh:mm:ssZ      | 1999-01-01T23:01:01Z      |
    +----------------------------------+---------------------------+---------------------------+


`SOQL Date Literal` - fieldExpression to compare a range of values to the value in a datetime 
field. Each literal is a range of time beginning with midnight (00:00:00). For example: `YESTERDAY`, `LAST_WEEK`, 
`LAST_MONTH` ...

**Duration:** SObject query duration filter applied to system field `LastModifiedDate` to allow range query. For example, if duration is '6' 
(6 hours) and the pipeline runs at 9am, it will read data updated from 3am-9am. 
Ignored if `datetimeFilter` is provided.

**Offset:** SObject query offset filter applied to system field `LastModifiedDate` to allow range query. For example, if duration is '6' 
(6 hours) and the offset is '1' (1 hour) and the pipeline runs at 9am, it will read data updated from 2am-8am.
Ignored if `datetimeFilter` is provided.

**Schema:** The schema of output objects.
