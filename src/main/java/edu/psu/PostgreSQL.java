package edu.psu;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class PostgreSQL {

    public static void create3NFStructure(String url, String user, String password) {

        String sql = "";
        String line = "";

        try (BufferedReader br = new BufferedReader(new FileReader("structurePostgreSQL.sql"))) {
            while ((line = br.readLine()) != null) {
                sql += line;
            }
        } catch (IOException e) {
            System.out.println("File not found");
        }

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage());
            System.exit(0);
        }

    }
}
