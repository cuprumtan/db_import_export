package edu.psu;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static edu.psu.SQLiteDB.*;
import static edu.psu.PostgreSQL.*;
import static edu.psu.Migration.*;
import static edu.psu.generatePostgreSQL.tables.Departments.DEPARTMENTS;

public class Main {

    static final String urlUNF = "jdbc:sqlite:perm_city_polyclinic_7_registry.db";
    static final String tableNameUNF = "perm_city_polyclinic_7_registry";

    static final String url3NF = "jdbc:postgresql://127.0.0.1:5432/perm_city_polyclinic_7_registry";
    static final String user3NF = "postgres";
    static final String password3NF = "postgres";

    public static void loadUNF() {
        createUNFStructure(urlUNF);
        insertIntoUNFTable(urlUNF, tableNameUNF);
    }

    public static void load3NF() {
        create3NFStructure(url3NF, user3NF, password3NF);
    }

    public static void main(String[] args) {
        //loadUNF();
        //load3NF();
        //migrateDataTo3NF(urlUNF, url3NF, user3NF, password3NF);
        export3NFCSV(url3NF, user3NF, password3NF);
    }
}
