package awesomesauce;

import java.sql.*;
import java.util.Locale;

public class poppin {
    private static Connection conn;

    public static void main(String[] args) {

        try {
            // Connection to DB
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1/distributeddb", "root", "");
            conn.setAutoCommit(false); // Defaults AutoCommit to false so that we have freedom over transactions
            System.out.println("Database connection is now established.");

            String table1 = "table1"; // table1: simple, no index, no partitioning
            String table2 = "table2"; // table2: index on col1, no partitioning
            String table3 = "table3"; // table3: no index, (4-part) partitioning on col1
            String table4 = "table4"; // table4: index on col1, (4-part) partitioning on col1


            // TABLE 1
            System.out.println(table1.toUpperCase(Locale.ROOT) + " - INITIAL:");
            System.out.println("Time it took to get number of records in " + table1 + ": " + getNumberOfRecordsInTable(table1) + " milliseconds"); // gets no. of records; returns long in milliseconds
            System.out.println("Insertion by single transaction took: " + insertIntoTable_singleTransaction(100000, table1) + " milliseconds");
            System.out.println("Insertion by multiple transaction took: " + insertIntoTable_multipleTransactions(100000, table1) + " milliseconds");
            System.out.println("This count function took: " + countWhere(table1, 1, 25) + " milliseconds");
            System.out.println("This count function took: " + countAllWhere_OR(table1, 25) + " milliseconds");
            System.out.println("This count function took: " + countAllWhere_AND(table1, 25) + " milliseconds");
            System.out.println(table1.toUpperCase(Locale.ROOT) + " - FINAL:");
            System.out.println("Time it took to get number of records in " + table1 + ": " + getNumberOfRecordsInTable(table1) + " milliseconds"); // gets no. of records; returns long in milliseconds
            //
            // TABLE2
            System.out.println(table2.toUpperCase(Locale.ROOT) + " - INITIAL:");
            System.out.println("Time it took to get number of records in " + table2 + ": " + getNumberOfRecordsInTable(table2) + " milliseconds"); // gets no. of records; returns long in milliseconds
            System.out.println("Insertion by single transaction took: " + insertIntoTable_singleTransaction(100000, table2) + " milliseconds");
            System.out.println("Insertion by multiple transaction took: " + insertIntoTable_multipleTransactions(100000, table2) + " milliseconds");
            System.out.println("This count function took: " + countWhere(table2, 1, 25) + " milliseconds");
            System.out.println("This count function took: " + countAllWhere_OR(table2, 25) + " milliseconds");
            System.out.println("This count function took: " + countAllWhere_AND(table2, 25) + " milliseconds");
            System.out.println(table2.toUpperCase(Locale.ROOT) + " - FINAL:");
            System.out.println("Time it took to get number of records in " + table2 + ": " + getNumberOfRecordsInTable(table2) + " milliseconds"); // gets no. of records; returns long in milliseconds
            //
            // TABLE3
            System.out.println(table3.toUpperCase(Locale.ROOT) + " - INITIAL:");
            System.out.println("Time it took to get number of records in " + table3 + ": " + getNumberOfRecordsInTable(table3) + " milliseconds"); // gets no. of records; returns long in milliseconds
            System.out.println("Insertion by single transaction took: " + insertIntoTable_singleTransaction(100000, table3) + " milliseconds");
            System.out.println("Insertion by multiple transaction took: " + insertIntoTable_multipleTransactions(100000, table3) + " milliseconds");
            System.out.println("This count function took: " + countWhere(table3, 1, 25) + " milliseconds");
            System.out.println("This count function took: " + countAllWhere_OR(table3, 25) + " milliseconds");
            System.out.println("This count function took: " + countAllWhere_AND(table3, 25) + " milliseconds");
            System.out.println(table3.toUpperCase(Locale.ROOT) + " - FINAL:");
            System.out.println("Time it took to get number of records in " + table3 + ": " + getNumberOfRecordsInTable(table3) + " milliseconds"); // gets no. of records; returns long in milliseconds
            //
            // TABLE4
            System.out.println(table4.toUpperCase(Locale.ROOT) + " - INITIAL:");
            System.out.println("Time it took to get number of records in " + table4 + ": " + getNumberOfRecordsInTable(table4) + " milliseconds"); // gets no. of records; returns long in milliseconds
            System.out.println("Insertion by single transaction took: " + insertIntoTable_singleTransaction(100000, table4) + " milliseconds");
            System.out.println("Insertion by multiple transaction took: " + insertIntoTable_multipleTransactions(100000, table4) + " milliseconds");
            System.out.println("This count function took: " + countWhere(table4, 1, 25) + " milliseconds");
            System.out.println("This count function took: " + countAllWhere_OR(table4, 25) + " milliseconds");
            System.out.println("This count function took: " + countAllWhere_AND(table4, 25) + " milliseconds");
            System.out.println(table4.toUpperCase(Locale.ROOT) + " - FINAL:");
            System.out.println("Time it took to get number of records in " + table4 + ": " + getNumberOfRecordsInTable(table4) + " milliseconds"); // gets no. of records; returns long in milliseconds
            //
            conn.close(); // Close DB connection
            System.out.println("Database connection is now closed.");
            //
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }


    }

    public static long countWhere(String tableName, int columnNumber, int condition) {
        try {
            long milliseconds_init = System.currentTimeMillis(); // Initial time: Current time of system in milliseconds
            PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) as COUNT from " + tableName + " WHERE col? = ?");
            stmt.setInt(1, columnNumber);
            stmt.setInt(2, condition);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            System.out.println("> Records of col" + columnNumber + " in " + tableName + " that satisfy condition " + condition + ": " + rs.getInt(1));
            long milliseconds_fin = System.currentTimeMillis(); // Final time
            return milliseconds_fin - milliseconds_init; // Returns Final time - Initial time = How much time it took to execute this function
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static long countAllWhere_OR(String tableName, int condition) {
        try {
            long milliseconds_init = System.currentTimeMillis(); // Initial time: Current time of system in milliseconds
            PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) as COUNT from " + tableName + " WHERE col1 = ? OR col2 = ? OR col3 = ?");
            stmt.setInt(1, condition);
            stmt.setInt(2, condition);
            stmt.setInt(3, condition);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            System.out.println("> Records in " + tableName + " that satisfy condition " + condition + " (OR): " + rs.getInt(1));
            long milliseconds_fin = System.currentTimeMillis(); // Final time
            return milliseconds_fin - milliseconds_init; // Returns Final time - Initial time = How much time it took to execute this function
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static long countAllWhere_AND(String tableName, int condition) {
        try {
            long milliseconds_init = System.currentTimeMillis(); // Initial time: Current time of system in milliseconds
            PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) as COUNT from " + tableName + " WHERE col1 = ? AND col2 = ? AND col3 = ?");
            stmt.setInt(1, condition);
            stmt.setInt(2, condition);
            stmt.setInt(3, condition);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            System.out.println("> Records in " + tableName + " that satisfy condition " + condition + " (AND): " + rs.getInt(1));
            long milliseconds_fin = System.currentTimeMillis(); // Final time
            return milliseconds_fin - milliseconds_init; // Returns Final time - Initial time = How much time it took to execute this function
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static long getNumberOfRecordsInTable(String tableName) { // Get number of records in database
        try {
            long milliseconds_init = System.currentTimeMillis(); // Initial time: Current time of system in milliseconds
            String SQLStatement = "SELECT COUNT(*) as COUNT FROM " + tableName;
            PreparedStatement stmt = conn.prepareStatement(SQLStatement);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            System.out.println("> Number of records in " + tableName + ": " + rs.getInt(1));
            long milliseconds_fin = System.currentTimeMillis(); // Final time
            return milliseconds_fin - milliseconds_init; // Returns Final time - Initial time = How much time it took to execute this function
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0;
    }

    public static long insertIntoTable_singleTransaction(int noOfRecords, String tableName) { // Inserts multiple records in single transaction via committing after multiple executions
        try {
            long milliseconds_init = System.currentTimeMillis(); // Initial time: Current time of system in milliseconds
            String SQLStatement = "INSERT INTO " + tableName + " (`col1`, `col2`, `col3`) VALUES (?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(SQLStatement);
            for(int i = 0; i < noOfRecords; i++) {
                stmt.setInt(1, (int) (Math.random() * 100));
                stmt.setInt(2, (int) (Math.random() * 100));
                stmt.setInt(3, (int) (Math.random() * 100));
                stmt.execute();
            }
            conn.commit(); // Commit executions
            System.out.println("> " + noOfRecords + " record(s) have been inserted into the database."); // Logging
            long milliseconds_fin = System.currentTimeMillis(); // Final time
            return milliseconds_fin - milliseconds_init; // Returns Final time - Initial time = How much time it took to execute this function
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0;
    }

    public static long insertIntoTable_multipleTransactions(int noOfRecords, String tableName) { // Inserts records in multiple transactions via committing after each execution
        try {
            long milliseconds_init = System.currentTimeMillis(); // Initial time: Current time of system in milliseconds
            String SQLStatement = "INSERT INTO " + tableName + " (`col1`, `col2`, `col3`) VALUES (?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(SQLStatement);
            for(int i = 0; i < noOfRecords; i++) {
                stmt.setInt(1, (int) (Math.random() * 100));
                stmt.setInt(2, (int) (Math.random() * 100));
                stmt.setInt(3, (int) (Math.random() * 100));
                stmt.execute();
                conn.commit(); // Commit executions
            }
            System.out.println("> " + noOfRecords + " record(s) have been inserted into the database."); // Logging
            long milliseconds_fin = System.currentTimeMillis(); // Final time
            return milliseconds_fin - milliseconds_init; // Returns Final time - Initial time = How much time it took to execute this function
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return 0;
    }


}
