package edu.psu;

import static edu.psu.SQLiteDB.*;
import static edu.psu.PostgreSQL.*;

public class Main {

    static final String urlUNF = "jdbc:sqlite:perm_city_polyclinic_7_registry.db";
    static final String tableNameUNF = "perm_city_polyclinic_7_registry";

    static final String url3NF = "jdbc:postgresql://127.0.0.1:5432/perm_city_polyclinic_7_registry";
    static final String user3NF = "postgres";
    static final String password3NF = "postgres";

    public static void loadUNF() {
        createNewDatabase(urlUNF);
        createNewTable(urlUNF, tableNameUNF);
        insertIntoTable(urlUNF, tableNameUNF);
    }

    public static void load3NF() {
        createNewDatabase(url3NF.substring(0, url3NF.lastIndexOf("/") + 1), user3NF, password3NF, url3NF.substring(url3NF.lastIndexOf("/") + 1));
        create3NFStructure(url3NF, user3NF, password3NF);
    }

    public static void main(String[] args) {
        loadUNF();
        load3NF();
    }
}
