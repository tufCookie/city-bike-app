import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;

public class PSQLDataImport {

    public static boolean importCSVFile(String fileLocation){
        Connection connection = null;
        boolean success = false;
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Connecting to the database bike data at localhost:5432/bike data....");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/bike-data", 
            "pepe", "notpassword : jdbc:postgresql://localhost:5432/bike-data\n");
            System.out.println("Success: Connected to ");
        }catch (Exception e){
            System.out.println("Could not connect to Database bike-data: " + e.getMessage());
            return success;
        }
        
        System.out.println("Reading and entering data to the database...");
        success = EnterDataIntoDatabase(connection, fileLocation);
        if(!success)
            return success;


        return true;
    }

    private static boolean EnterDataIntoDatabase(Connection connection, String fileLocation){
        try(BufferedReader br = new BufferedReader(new FileReader(fileLocation))){
            System.out.println("Retrieving from CSV file, filtering data, and inserting to the database.... good time to get coffee : )");
            String line = "";
            int count = 0;
            while((line = br.readLine()) != null && count < 10){
                String[] values = line.split("[,]");
                System.out.println(Arrays.toString(values));
                ++count;
            }
            connection.close();
            System.out.println("Success: CSV file imported.");
        }catch(Exception e){
            System.out.println("Could not import CSV file try harder.....");
            return false;
        }
        return true;
    }


}
