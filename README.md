# Cloudextraction

Compile the project into a jar file under ```target```.

Then upload to lambda.

## Handler: 
```lambda.Extaction::handleRequest```

## Runtime: 
```Java8```

## Memory(MB): 
```512MB``` at least

### Timeout
depend, start with 1 or 2 mins, could go up to 15 mins.


## Environment Variables
Your lambda will need these environment variables (Add environment vars on ur Lambda UI, under the upload for source code section): 

```dbName : my_db.db```

```tableName : my_table```

```filterd: filterd/ ``` 


## example:

   ```  private static final String dbName = "Add you dbName"; ```
   
   ```  private static final String tableName = "Add your tableName"; ```
   
   ```  private static final String filterd ="filterd/"; ```
   
## Request
Request body required ```bucketname``` and ```objectKey```
 

```
{
  "bucketname" : "your bucketname",
  "objectKey" : "location of save query output files"
}
```
## Request _ example:

```
{
  "bucketName": "tcss562.mylogs.ali",
  "objectKey": "loaded/sale.db"
}
```

## Response _ example:
```
{
  "uuid": "012cbe00-1c22-4cf8-baeb-e2ecfc37e06c",
  "error": "",
  "vmuptime": 1544199174,
  "newcontainer": 1,
  "success": true,
  "bucketname": "tcss562.mylogs.ali",
  "tablename": "sale_table",
  "dbname": "sale.db",
  "fname_filtering": "20181207_162345_filtering.csv",
  "fname_aggregate": "20181207_162345_aggregate.csv"
}
```

