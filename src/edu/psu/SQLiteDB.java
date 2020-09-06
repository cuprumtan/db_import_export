package edu.psu;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLiteDB {

    public static void createNewDatabase(String url) {

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new SQLite database has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void createNewTable(String url) {

        String sql = "CREATE TABLE IF NOT EXISTS perm_city_polyclinic_7_registry (\n" +
                "name_doctor VARCHAR(50),\n" +
                "birth_date_doctor TEXT,\n" +
                "sex_doctor CHARACTER(1),\n" +
                "education_doctor VARCHAR(16),\n" +
                "position VARCHAR(40),\n" +
                "qualification_category CHARACTER(4),\n" +
                "department VARCHAR(64),\n" +
                "special_department VARCHAR(64),\n" +
                "name_patient VARCHAR(50),\n" +
                "birth_date_patient TEXT,\n" +
                "sex_patient CHARACTER(1)\n" +
                "card_number INTEGER,\n" +
                "visit_datetime TEXT,\n" +
                "visit_status INTEGER,\n" +
                "visit_comment TEXT);";

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void insertIntoTable(String url, String tableName, String data) {

        String sql = "INSERT INTO perm_city_polyclinic_7_registry VALUES VALUES " + data + ";";

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
