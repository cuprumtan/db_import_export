package edu.psu;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class PostgreSQL {

    public static void createNewDatabase(String url, String user, String password, String dbName) {

        String sql = "CREATE DATABASE " + dbName + ";";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                stmt.executeUpdate(sql);
                conn.close();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new PostgreSQL database has been created");
            }
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            System.exit(0);
        }

    }

    public static void create3NFStructure(String url, String user, String password) {

        String sql = "";
        String line = "";

        try (BufferedReader br = new BufferedReader(new FileReader("structure.sql"))) {
            while ((line = br.readLine()) != null) {
                sql += line;
            }
        } catch (IOException e) {
            System.out.println("File not found");
        }

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            System.exit(0);
        }

    }
}
