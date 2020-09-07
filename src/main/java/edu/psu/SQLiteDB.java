package edu.psu;

import edu.psu.generateSQLite.tables.PermCityPolyclinic_7Registry;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SQLiteDB {

    public static void createUNFStructure(String url) {

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new SQLite database has been created");

                String line = "";
                String sql = "";

                try (BufferedReader br = new BufferedReader(new FileReader("structureSQLite.sql"))) {
                    while ((line = br.readLine()) != null) {
                        sql += line;
                    }
                    stmt.executeUpdate(sql);
                } catch (IOException e) {
                    System.out.println("File not found");
                }
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public static List<List<String>> loadCSV(String path) {
        String line = "";
        String cvsSplitBy = ",";

        List<List<String>> arrayList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] values = line.split(cvsSplitBy);
                arrayList.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            System.out.println("File not found");
        }

        return arrayList;
    }

    public static List<List<String>> loadData() {

        List<List<String>> doctors = loadCSV("csv/doctors.csv");
        List<List<String>> patients = loadCSV("csv/patients.csv");

        String[] dates = {"2020-09-05 09:00:00", "2020-09-05 10:00:00", "2020-09-05 11:00:00", "2020-09-05 12:00:00", "2020-09-05 14:00:00", "2020-09-05 16:00:00", "2020-09-05 17:00:00",
                          "2020-09-06 09:00:00", "2020-09-06 10:00:00", "2020-09-06 11:00:00", "2020-09-06 12:00:00", "2020-09-06 14:00:00", "2020-09-06 16:00:00", "2020-09-06 17:00:00",};

        List<Integer> patientsNumbers;

        List<List<String>> registry = new ArrayList<>();
        Random random = new Random();
        int randomPatient;

        for (int i=0; i < doctors.size(); i++)
        {
            patientsNumbers = IntStream.range(1, patients.size()).boxed().collect(Collectors.toList());
            for (int j=0; j < dates.length && j < patientsNumbers.size(); j++)
            {
                List<String> row = new ArrayList<>();
                row.addAll(doctors.get(i));
                randomPatient = patientsNumbers.get(random.nextInt(patientsNumbers.size()));
                row.addAll(patients.get(randomPatient));
                patientsNumbers.remove(patientsNumbers);
                row.add(dates[j]);
                row.add("1");
                row.add("Назначен повторный прием через 2 недели.");
                registry.add(row);
            }
        }

        return registry;
    }

    public static void insertIntoUNFTable(String url, String tableName) {

        List<List<String>> data = loadData();

        try (Connection conn = DriverManager.getConnection(url)) {

            DSLContext context = DSL.using(conn);
            for (int i=0; i < data.size(); i++)
            {
                context.insertInto(PermCityPolyclinic_7Registry.PERM_CITY_POLYCLINIC_7_REGISTRY)
                        .values(data.get(i).get(0), data.get(i).get(1), data.get(i).get(2), data.get(i).get(3),
                                data.get(i).get(4), data.get(i).get(5), data.get(i).get(6), data.get(i).get(7),
                                data.get(i).get(8), data.get(i).get(9), data.get(i).get(10), data.get(i).get(11),
                                data.get(i).get(12), data.get(i).get(13), data.get(i).get(14))
                        .execute();
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

}
