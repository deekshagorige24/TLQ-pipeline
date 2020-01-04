/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import saaf.Inspector;
import saaf.Response;
import com.amazonaws.services.lambda.runtime.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
/**
 *
 * @author Deeksha Rao Gorige
 */
public class S3Query implements RequestHandler<Request, HashMap<String, Object>>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    
    
    // Lambda Function Handler
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        // Create logger
        LambdaLogger logger = context.getLogger();
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        inspector.addTimeStamp("frameworkRuntime");
        
        //****************START FUNCTION IMPLEMENTATION*************************

        //Create and populate a separate response object for function output. (OPTIONAL)
        Response r = new Response();
        
        try 
        {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
            
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");

            Connection con = DriverManager.getConnection(url,username,password);

            PreparedStatement ps = con.prepareStatement("SELECT SUM(Units_Sold) as sum FROM SalesDB ;");
            ResultSet rs = ps.executeQuery();

            ps = con.prepareStatement("select AVG(Units_Sold) as Avg from SalesDB;");
            ResultSet sr = ps.executeQuery();
            
            ps = con.prepareStatement("select MIN(Units_Sold) as Min from SalesDB;");
            ResultSet min = ps.executeQuery();
            
            ps = con.prepareStatement("select SUM(Total_Revenue) as Revenue from SalesDB;");
            ResultSet rev = ps.executeQuery();

            ps = con.prepareStatement("select SUM(Total_Profit) as Profit from SalesDB;");
            ResultSet pro = ps.executeQuery();

            ps = con.prepareStatement("select COUNT(Order_ID) as Count from SalesDB;");
            ResultSet cou = ps.executeQuery();

            ps = con.prepareStatement(" select Order_ID as ID from SalesDB where Unit_Price between 205.70 and 437.20;");
            ResultSet query1 = ps.executeQuery();

            ps = con.prepareStatement("select Region as region from SalesDB where Item_Type = 'Cereal'");
            ResultSet query2 = ps.executeQuery();

            ps = con.prepareStatement("select MAX(Units_Sold) as Max from SalesDB;");
            ResultSet max = ps.executeQuery();

            ps = con.prepareStatement("select Country, sum(Total_Profit) as Profits from SalesDB group by Country;");
            ResultSet query3 = ps.executeQuery();
            
            LinkedList<String> ll1 = new LinkedList<String>();
            rs.next();
            logger.log("Sum(Units_Sold)=" + rs.getString("sum"));
            ll1.add(rs.getString("sum"));
            rs.close();
            
            LinkedList<String> ll2 = new LinkedList<String>();
            sr.next();
            logger.log("Average(Units_Sold)=" + sr.getString("Avg"));
            ll2.add(sr.getString("Avg"));
            sr.close();

            LinkedList<String> ll3 = new LinkedList<String>();
            min.next();
            logger.log("Minumum(Units_Sold)=" + min.getString("Min"));
            ll3.add(min.getString("Min"));
            min.close();

            LinkedList<String> ll4 = new LinkedList<String>();
            rev.next();
            logger.log("Sum(Total_Revenue)=" + rev.getString("Revenue"));
            ll4.add(rev.getString("Revenue"));
            rev.close();

            LinkedList<String> ll5 = new LinkedList<String>();
            pro.next();
            logger.log("Sum(Total_Profit)=" + pro.getString("Profit"));
            ll5.add(pro.getString("Profit"));
            pro.close();

            LinkedList<String> ll6 = new LinkedList<String>();
            cou.next();
            logger.log("Count(Total_Orders)=" + cou.getString("Count"));
            ll6.add(cou.getString("Count"));
            cou.close();

            LinkedList<String> ll7 = new LinkedList<String>();
            while (query1.next())
            {
            logger.log("Output_with_where_clause=" + query1.getString("ID"));
            ll7.add(query1.getString("ID"));
            }
            query1.close();

            LinkedList<String> ll8 = new LinkedList<String>();
            while(query2.next())
            {
            logger.log("Query2=" + query2.getString("region"));
            ll8.add(query2.getString("region"));
            }
            query2.close();

            LinkedList<String> ll9 = new LinkedList<String>();
            max.next();
            logger.log("Maximum(Units_Sold)=" + max.getString("Max"));
            ll9.add(max.getString("Max"));
            max.close();

            LinkedList<String> ll10 = new LinkedList<String>();
            while(query3.next())
            {
            logger.log("Query3=" + query3.getString("Profits"));
            ll10.add(query3.getString("Profits"));
            }
            query3.close();
            
            
            con.close();
            r.setSum(ll1);
            r.setAverage(ll2);
            r.setMinimum(ll3);
            r.setRevenue(ll4);
            r.setProfit(ll5);
            r.setCount(ll6);
            r.setQuery1(ll7);
            r.setQuery2(ll8);
            r.setMaximum(ll9);
            r.setQuery3(ll10);

        } 
        catch (Exception e) 
        {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
        }
        
        String hello = "Hello"; //+ request.getName();

        //Print log information to the Lambda log as needed
        //logger.log("log message...");
        
        // Set return result in Response class, class is marshalled into JSON
        r.setValue(hello);
         
        
        //****************END FUNCTION IMPLEMENTATION***************************
        //inspector.consumeResponse(r);
        
        //Collect final information such as total runtime and cpu deltas.
        //inspector.inspectAllDeltas();
        inspector.consumeResponse(r);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    
    // int main enables testing function from cmd line
    public static void main (String[] args)
    {
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
        
        // Create an instance of the class
        S3Query lt = new S3Query();
        
        // Create a request object
        Request req = new Request();

        // Grab the name from the cmdline from arg 0
        //String name = (args.length > 0 ? args[0] : "");

        // Load the name into the request object
        //req.setName(name);

        // Report name to stdout
        //System.out.println("cmd-line param name=" + req.getName());

        // Test properties file creation
        Properties properties = new Properties();
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("url","");
        properties.setProperty("username","");
        properties.setProperty("password","");
        try 
        {
          properties.store(new FileOutputStream("test.properties"),"");
        }
        catch (IOException ioe)
        {
          System.out.println("error creating properties file.")   ;
        }
        
        
        // Run the function
        //Response resp = lt.handleRequest(req, c);
        System.out.println("The MySQL Serverless can't be called directly without running on the same VPC as the RDS cluster.");
        Response resp = new Response();
        
        // Print out function result
        System.out.println("function result:" + resp.toString());
        
    }
}
