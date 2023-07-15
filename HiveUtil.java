import java.sql.*;

public class HiveUtil {
    private static final String HIVE_CONNECTION_URL = "jdbc:hive2://master:10100/default";

    private static Connection getConnection() throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(HIVE_CONNECTION_URL, "ph", "");
    }

    public static void createTable(String tableName, String columns) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            String sql = String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, columns);
            stmt.execute(sql);
            System.out.println("Table created successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void dropTable(String tableName) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            String sql = String.format("DROP TABLE IF EXISTS %s", tableName);
            stmt.execute(sql);
            System.out.println("Table dropped successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void insertData(String tableName, String values) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            String sql = String.format("INSERT INTO TABLE %s VALUES (%s)", tableName, values);
            stmt.execute(sql);
            System.out.println("Data inserted successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void queryData(String tableName) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            String sql = String.format("SELECT * FROM %s", tableName);
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                // Process the row data
                // Example:
                String column1Value = resultSet.getString("column1");
                int column2Value = resultSet.getInt("column2");
                System.out.println(column1Value + "\t" + column2Value);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void updateData(String tableName, String condition, String updateValues) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            String sql = String.format("UPDATE %s SET %s WHERE %s", tableName, updateValues, condition);
            stmt.execute(sql);
            System.out.println("Data updated successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void deleteData(String tableName, String condition) {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            String sql = String.format("DELETE FROM %s WHERE %s", tableName, condition);
            stmt.execute(sql);
            System.out.println("Data deleted successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
