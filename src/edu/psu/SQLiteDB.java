package edu.psu;

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

    public static void createNewTable(String url, String tableName) {

        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                "name_doctor VARCHAR(50) NOT NULL,\n" +
                "birth_date_doctor TEXT NOT NULL,\n" +
                "sex_doctor CHARACTER(1) NOT NULL,\n" +
                "education_doctor VARCHAR(16) NOT NULL,\n" +
                "position VARCHAR(40) NOT NULL,\n" +
                "qualification_category CHARACTER(4) NOT NULL,\n" +
                "department VARCHAR(64) NOT NULL,\n" +
                "special_department VARCHAR(64) NOT NULL,\n" +
                "name_patient VARCHAR(50) NOT NULL,\n" +
                "birth_date_patient TEXT NOT NULL,\n" +
                "sex_patient CHARACTER(1) NOT NULL,\n" +
                "card_number INTEGER NOT NULL,\n" +
                "visit_datetime TEXT NOT NULL,\n" +
                "visit_status INTEGER NOT NULL,\n" +
                "visit_comment TEXT NULL);";

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
            System.out.println("Table " + tableName + " created");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static String formateData(List<List<String>> arrayList) {
        String formatedData = "";

        for (int row=0; row < arrayList.size(); row++)
        {
            formatedData += "('";
            for (int col=0; col < arrayList.get(row).size(); col++)
            {
                String value = arrayList.get(row).get(col);
                formatedData += value + "', '";
            }
            formatedData = formatedData.substring(0, formatedData.length() - 3) + "), ";
        }
        return formatedData.substring(0, formatedData.length() - 2) + "; ";
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
            e.printStackTrace();
        }

        return arrayList;
    }

    public static String loadData() {

        String dataValues = "";

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
                row.add("t");
                row.add("Назначен повторный прием через 2 недели.");
                registry.add(row);
            }
        }

        dataValues = formateData(registry);
        // System.out.println(dataValues);

        return dataValues;
    }

    public static void insertIntoTable(String url, String tableName) {

        String data = loadData();

        String sql = "INSERT INTO " + tableName + " VALUES " + data;

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}
