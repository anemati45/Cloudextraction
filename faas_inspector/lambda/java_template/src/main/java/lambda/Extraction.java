/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;

import java.io.File;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import faasinspector.register;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.SdkClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author wlloyd
 */
public class Extraction implements RequestHandler<Request, Response> {

//    private static final DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    LambdaLogger logger = null;
    public AmazonS3 s3client;
    private static final String LAMBDA_TEMP_DIRECTORY = "/tmp/";
    private static final String AWS_REGION = "us-east-1";
//    private static final String TRANSFORM = "transformed"; //after lambda 1 transform, put csv here.
//    private static final String LOAD = "loaded"; // after lambda 2 (this) load db, upload db file here.
//    private static final String CSV_DELIM = ",";

//    
//    private static final String dbName = "sale.db";
    //  private static final String tableName = "sale_table";
    // private static final String filterd = "filterd/";
//    
//    "bucketname":"tcss562.mylogs.ali",
//"dbname" : "sale.db",
//"tablename" : "sale_table",
//"transactionid" : "<passing along>"
//    private static final String filterd = System.getenv("FILTERD");
//    private static final String objectKey =System.getenv("objectKey");//loaded/sale.db
//    private static final String filterd = System.getenv("FILTERD");
//        String objectKey = "loaded/";
    String objectKey = System.getenv("objectKey"); //"filtered/" 


    public Response handleRequest(Request request, Context context) {

        String bucketName = request.getbucketname();
        String dbName = request.getdbname();
        String tableName = request.gettablename();
//        String transactionid= request.getDbname();
//         
//        String filterd = "filterd/";
        //static path to csv file under /tmp to be loaded to db table:
        String csvFilePath = LAMBDA_TEMP_DIRECTORY +"loaded" +"/"+ dbName;
//        System.out.println("bucketName " + bucketName + "  dbName  "+dbName+"      objectKey:  " + objectKey + " csvFilePath :" + csvFilePath);
        // Create logger
        logger = context.getLogger();

        //setup S3 client :
        s3client = AmazonS3ClientBuilder.standard().withRegion(AWS_REGION).build();

        listFile(LAMBDA_TEMP_DIRECTORY);
        register reg = new register(logger);

        //stamp container with uuid
        Response r = reg.StampContainer();

        setCurrentDirectory("/tmp");
//
        String precheckErrMsg = validateParams(request);
        if (precheckErrMsg != null) {
            setResponseObj(r, false, precheckErrMsg, null, null, null);
            return r;
        }
        logger.log("input fileName: " + bucketName);
                  

        // *********************************************************************
        // Implement Lambda Function Here
        // *********************************************************************
        try {
            // get file from s3, download to /tmp
            File tmpDir = new File(csvFilePath);
            boolean exists = tmpDir.exists();
            if (!exists) {
                getDataFromS3(bucketName, "loaded"+"/" + dbName);
            }
            listFile(LAMBDA_TEMP_DIRECTORY);

            String url = "jdbc:sqlite:" + dbName;
            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection(url);

            // Detect if the table  exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT * FROM sqlite_master WHERE type='table' AND name='" + tableName + "'");

            ResultSet rs = ps.executeQuery();

            //list file for debugging:
            listFile(LAMBDA_TEMP_DIRECTORY);
            ps = con.prepareStatement("select \"Region\",\"Country\", \"Item Type\", "
                    + "avg(\"Units Sold\"), min(\"Units Sold\"),  max(\"Units Sold\"),\n"
                    + "                       sum(\"Units Sold\"), count(\"Units Sold\") from "
                    + tableName + "  WHERE  \"Item Type\" = \"Clothes\";");
//            ps = con.prepareStatement("select * from sale_table;");
            rs = ps.executeQuery();
            String timeStamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(Calendar.getInstance().getTime());
            UUID uid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d").randomUUID();

            String filesave_filtering = objectKey + timeStamp + "-" + uid + "-filtering.csv";
//            System.out.print(" filesave_filtering is  ################" +filesave_filtering);
//            filesave_filtering = filterd + filesave_filtering;

            saveData(rs, bucketName, filesave_filtering);

            ps = con.prepareStatement("select \"Region\",\"Country\", \"Item Type\", "
                    + "avg(\"Units Sold\"), min(\"Units Sold\"),  max(\"Units Sold\"),\n"
                    + "                       sum(\"Units Sold\"), count(\"Units Sold\") from "
                    + tableName + "  group by \"Region\",\"Country\", \"Item Type\";");
//            ps = con.prepareStatement("select * from sale_table;");
            rs = ps.executeQuery();
//            listFile(LAMBDA_TEMP_DIRECTORY);

//            System.out.println(timeStamp);
            UUID uid2 = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d").randomUUID();
            String filesave_aggregate = objectKey + timeStamp + "-" + uid2 + "-aggregate.csv";

//            System.out.print(" filesave_aggregate is  ################" +filesave_aggregate);
//            filesave_aggregate = filterd + filesave_aggregate;
            saveData(rs, bucketName, filesave_aggregate);
//        

//            String s = filesave_aggregate;
            String filesave_aggregate_no_slash = filesave_aggregate.substring(filesave_aggregate.indexOf("/") + 1);
            filesave_aggregate_no_slash.trim();

            String filesave_filtering_no_slash = filesave_filtering.substring(filesave_filtering.indexOf("/") + 1);
            filesave_filtering_no_slash.trim();
            //send response
            setResponseObj(r, true, null, bucketName, filesave_filtering, filesave_filtering_no_slash);
            // Delete the sample objects.

//            setResponseObj(r, true, null, bucketName, filesave_filtering, filesave_aggregate);
//             delete file in s3
            try {

                s3client.deleteObject(bucketName, filesave_aggregate);
                s3client.deleteObject(bucketName, filesave_filtering);

            } catch (AmazonServiceException e) {
                System.err.println(e.getErrorMessage());
                System.exit(1);
            }
            rs.close();
            con.close();

        } catch (SQLException sqle) {
            logger.log("DB ERROR:" + sqle.toString());

        } catch (Exception e) {
            logger.log("File Error: " + e.toString());
            setResponseObj(r, false, e.toString(), null, null, null);

        }

//        logger.log("log R:" + r);
        return r;
    }

    //insert data from csv to table
    /**
     * Helper method
     *
     * @param directory_name
     * @return boolean
     */
    private static boolean setCurrentDirectory(String directory_name) {
        boolean result = false;  // Boolean indicating whether directory was set
        File directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs()) {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }

    
    
    //list File under a path:
    public static void listFile(String path){
        System.out.println("========== listFile under: " + path);
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles.length > 0 ) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    System.out.println("File " + listOfFiles[i].getName());

                } else if (listOfFiles[i].isDirectory()) {
                    System.out.println("Directory " + listOfFiles[i].getName());
                }
            }
        }

        System.out.println("=============================================");
    }

    
    private void setResponseObj(Response r, boolean success, String e,
            String dbName, String filesave_filtering, String filesave_aggregate) {
        // Set response object:
//        r.setBucketname(bucketName);
        r.setDbname(dbName);

        if (success) {
            r.setSuccess(true);
//            r.setBucketname(bucketName);
            r.setDbname(dbName);
//            r.setTablename(tableName);
            r.setFname_filtering(filesave_filtering);
            r.setFname_aggregate(filesave_aggregate);
        } else {
            r.setSuccess(false);
            r.setError(e);
        }

    }

//    SAVE DATA TO S3 --------------------------------------------
    private void saveData(ResultSet resultSet, String bucketName, String filesave) {
        logger.log("Save data in S3 ");
        try {
//            int count = 0;
            ResultSetMetaData rsmd = resultSet.getMetaData();
//            String result1 = "";
            int columnsNumber = rsmd.getColumnCount();
            StringWriter result = new StringWriter();

            result = result.append("Region,Country,Item_Type,Average_of_Units_Sold,Min_of_Units_Sold, Max_of_Units_Sold,Sum_of_Units_Sold,Count_of_Units_Sold");
//            System.out.println(result);
            result.append("\n");
            while (resultSet.next()) {
//                count += 1;
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) {
//                        System.out.print(",");
                    }
                    String columnValue = resultSet.getString(i);
                    result.append(columnValue);
                    if ((i) != columnsNumber) {
                        result.append(",");
                    } else {
                        result.append("\n");
                    }
//                    System.out.print(result);
                }
            }
//            System.
            byte[] bytes = result.toString().getBytes(StandardCharsets.UTF_8);
            InputStream is = new ByteArrayInputStream(bytes);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentType("text/plain");
            // Create new file on S3
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            s3Client.putObject(bucketName, filesave, is, meta);

        } catch (SQLException e) {
            System.out.println("Error displayData. " + e.getMessage());
        }

    }

    /**
     * get s3 object
     *
     * @param bucketName bucket name
     * @param objectKey object key aka name of file in s3, or path of file in s3
     */
    private void getDataFromS3(String bucketName, String objectKey) {
//        System.out.println("getting file from s3 for " + bucketName + " : " + objectKey);
        try {
            s3client.getObject(new GetObjectRequest(bucketName, objectKey),
                    new File(LAMBDA_TEMP_DIRECTORY + "sale.db"));
        } catch (Exception e) {
            logger.log("Error getting object from S3. " + e.getMessage());
        }

    }

    //just do some simple validation for required fields:
    private String validateParams(Request request) {

        if ((request.bucketname) == null || request.getbucketname().isEmpty()) {
            return "\"bucketName\" is required in request";
        }

        if ((request.dbname) == null || (request.getdbname().isEmpty())) {
            return "\"dbName\" is required in request";
        }

        if ((request.tablename) == null || request.gettablename().isEmpty()) {
            return "\"tableName\" is required in request.";

        }
//


//        if (filterd == null || filterd.isEmpty()) {
//            return "Environment variables:\"filterd\" is not set";
//        }
        
           if (objectKey == null || objectKey.isEmpty()) {
            return "Environment variables:\"objectKey\" is not set";
        }

        return null;
    }

    //list File under a path:
//    public static void listFile(String path) {
////        System.out.println("========== listFile under:================= " + path);
//        File folder = new File(path);
//        File[] listOfFiles = folder.listFiles();
//        if (listOfFiles.length > 0) {
//            for (int i = 0; i < listOfFiles.length; i++) {
//                if (listOfFiles[i].isFile()) {
//                    System.out.println("File " + listOfFiles[i].getName());
//
//                } else if (listOfFiles[i].isDirectory()) {
//                    System.out.println("Directory " + listOfFiles[i].getName());
//                }
//            }
//        }
//
////        System.out.println("=============================================");
//    }
    // TODO: fix this so we can collect metrics:
    // int main enables testing function from cmd line
    public static void main(String[] args) {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

//        Extraction lt = new Extraction();

        // Create a request object
//        Request req = new Request();
//
////         Grab the name from the cmdline from arg 0
//        String bucketName = (args.length > 0 ? args[0] : "");
//
//        // Load the name into the request object
//        req.setBucketName(bucketName);
//
//        // Grab the name from the cmdline from arg 0
//        String dbName = (args.length > 0 ? args[1] : "");
////
////        // Load the name into the request object
//        req.setDdname(dbName);
//
//        String tableName = (args.length > 0 ? args[2] : "");
////
////        // Load the name into the request object
//        req.setTableName(tableName);
////        // Report name to stdout
//        // Grab the name from the cmdline from arg 0
//
//        System.out.println("cmd-line param name=" + req.getDbname());
//
//        // Run the function
//        Response resp = lt.handleRequest(req, c);
//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException ie) {
//            System.out.print(ie.toString());
//        }
//        // Print out function result
//        System.out.println("function result:" + resp.toString());
    }
}
