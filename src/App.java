public class App {
    public static void main(String[] args) throws Exception {
        String file = "C:\\Users\\prato\\Repos\\setup-database-fin\\bike-data\\bike-data-1.csv";
        PSQLDataImport.importCSVFile(file);
    }
}
