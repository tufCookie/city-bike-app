import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PSQLDataImport {

    /*
     * Initializes postgres DB, reads and stores data, and closes the connection
     */
    public static boolean importCSVFile(String fileLocation) {
        Connection connection = null;
        boolean success = false;
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Connecting to the database bike data at localhost:5432/bike data....");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/bike-data",
                    "postgres", "rooty@420");
            System.out.println("Success: Connected to jdbc:postgresql://localhost:5432/bike-data.");
            System.out.println("Reading and entering data to the database...");
            success = EnterDataIntoDatabase(connection, fileLocation);
        } catch (Exception e) {
            System.out.println("Could not connect to Database bike-data: " + e.getMessage());
            return false;
        }
        return success;
    }

    /*
     * Reads data from CSV file and enters it into the postgres DB
     */
    private static boolean EnterDataIntoDatabase(Connection connection, String fileLocation) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileLocation),
                StandardCharsets.UTF_8))) {
            System.out.println(
                    "Retrieving from CSV file, filtering data, and inserting to the database.... good time to get coffee : )");
            String line = br.readLine();

            String SQL = "INSERT INTO journey(departure, return, departure_station_id, departure_station_name, return_station_id, return_station_name, distance, duration) "
                    + "VALUES(?,?,?,?,?,?,?,?)";
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                int duration = Integer.parseInt(values[7]);
                Float distance = Float.parseFloat(values[6]);
                if (duration >= 10 && distance >= 0) {
                    Timestamp departureDate = GetTimeStamp(values[0]);
                    Timestamp returnDate = GetTimeStamp(values[1]);
                    if (departureDate == null || returnDate == null) {
                        continue;
                    }

                    PreparedStatement pstmt = connection.prepareStatement(SQL);
                    pstmt.setTimestamp(1, departureDate);
                    pstmt.setTimestamp(2, returnDate);
                    pstmt.setInt(3, Integer.parseInt(values[2]));
                    pstmt.setString(4, values[3]);
                    pstmt.setInt(5, Integer.parseInt(values[4]));
                    pstmt.setString(6, values[5]);
                    pstmt.setFloat(7, Float.parseFloat(values[6]));
                    pstmt.setInt(8, Integer.parseInt(values[7]));

                    int affectedRows = pstmt.executeUpdate();
                    if (affectedRows == 0)
                        throw new Exception("Error adding entry..." + values.toString());
                }
            }
            connection.close();
            System.out.println("Success: CSV file imported.");
        } catch (Exception e) {
            System.out.println("Could not import CSV file try harder....." + e.getMessage());
            return false;
        }
        return true;
    }

    /*
     * Formats the date to enter into postgres DB
     */
    private static Timestamp GetTimeStamp(String timeString) {
        try {
            Date date = (Date) new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(timeString);
            Timestamp timestamp = new Timestamp(date.getTime());
            return timestamp;
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            return null;
        }
    }
}
