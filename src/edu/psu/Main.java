package edu.psu;

import static edu.psu.SQLiteDB.*;

public class Main {

    public static void main(String[] args) {
        String url = "jdbc:sqlite:perm_city_polyclinic_7_registry.db";
        String tableName = "perm_city_polyclinic_7_registry";
        createNewDatabase(url);
        createNewTable(url, tableName);
        insertIntoTable(url, tableName);
    }
}
