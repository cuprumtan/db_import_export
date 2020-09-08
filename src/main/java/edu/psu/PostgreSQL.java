package edu.psu;

import edu.psu.generatePostgreSQL.tables.pojos.*;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.io.*;
import java.sql.*;
import java.util.List;

import static edu.psu.generatePostgreSQL.tables.Departments.DEPARTMENTS;
import static edu.psu.generatePostgreSQL.tables.Doctors.DOCTORS;
import static edu.psu.generatePostgreSQL.tables.Patients.PATIENTS;
import static edu.psu.generatePostgreSQL.tables.QualificationCategories.QUALIFICATION_CATEGORIES;
import static edu.psu.generatePostgreSQL.tables.SpecialDepartments.SPECIAL_DEPARTMENTS;
import static edu.psu.generatePostgreSQL.tables.Visits.VISITS;

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
        catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public static void writeDepartmentsToCSV(String path, List<Departments> data, String fileName) {
        try {

            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(path + fileName));

            // write column names
            fileWriter.write("ID,department");

            for (int i=0; i < data.size(); i++) {
                String row = data.get(i).getId() + "," +
                        data.get(i).getDepartment();
                fileWriter.newLine();
                fileWriter.write(row);
            }
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("File IO error:");
            e.printStackTrace();
        }
    }

    public static void writeDoctorsToCSV(String path, List<Doctors> data, String fileName) {
        try {

            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(path + fileName));

            // write column names
            fileWriter.write("ID,last_name,first_name,patronymic,birth_date,sex,education,position,id_qc,id_sp");

            for (int i=0; i < data.size(); i++) {
                String row = data.get(i).getId() + "," +
                        data.get(i).getLastName() + "," +
                        data.get(i).getFirstName() + "," +
                        data.get(i).getPatronymic() + "," +
                        data.get(i).getBirthDate() + "," +
                        data.get(i).getSex() + "," +
                        data.get(i).getEducation() + "," +
                        data.get(i).getPosition() + "," +
                        data.get(i).getIdQualificationCategory() + "," +
                        data.get(i).getIdSpecialDepartment();
                fileWriter.newLine();
                fileWriter.write(row);
            }
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("File IO error:");
            e.printStackTrace();
        }
    }

    public static void writePatientsToCSV(String path, List<Patients> data, String fileName) {
        try {

            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(path + fileName));

            // write column names
            fileWriter.write("card_number,last_name,first_name,patronymic,birth_date,sex");

            for (int i=0; i < data.size(); i++) {
                String row = data.get(i).getCardNumber() + "," +
                        data.get(i).getLastName() + "," +
                        data.get(i).getFirstName() + "," +
                        data.get(i).getPatronymic() + "," +
                        data.get(i).getBirthDate() + "," +
                        data.get(i).getSex();
                fileWriter.newLine();
                fileWriter.write(row);
            }
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("File IO error:");
            e.printStackTrace();
        }
    }

    public static void writeQualificationCategoriesToCSV(String path, List<QualificationCategories> data, String fileName) {
        try {

            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(path + fileName));

            // write column names
            fileWriter.write("id,qa");

            for (int i=0; i < data.size(); i++) {
                String row = data.get(i).getId() + "," +
                        data.get(i).getQualificationCategory();
                fileWriter.newLine();
                fileWriter.write(row);
            }
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("File IO error:");
            e.printStackTrace();
        }
    }

    public static void writeSpecialDepartmentsToCSV(String path, List<SpecialDepartments> data, String fileName) {
        try {

            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(path + fileName));

            // write column names
            fileWriter.write("id,special_department,id_department");

            for (int i=0; i < data.size(); i++) {
                String row = data.get(i).getId() + "," +
                        data.get(i).getSpecialDepartment() + "," +
                        data.get(i).getIdDepartment();
                fileWriter.newLine();
                fileWriter.write(row);
            }
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("File IO error:");
            e.printStackTrace();
        }
    }

    public static void writeVisitsToCSV(String path, List<Visits> data, String fileName) {
        try {

            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(path + fileName));

            // write column names
            fileWriter.write("id,special_department,id_department");

            for (int i=0; i < data.size(); i++) {
                String row = data.get(i).getVisitDatetime() + "," +
                        data.get(i).getIdDoctor() + "," +
                        data.get(i).getCardNumber() + "," +
                        data.get(i).getStatus() + "," +
                        // field can be null, but it is in last column so we can parse it like other fields
                        data.get(i).getVisitComment();
                fileWriter.newLine();
                fileWriter.write(row);
            }
            fileWriter.close();

        } catch (IOException e) {
            System.out.println("File IO error:");
            e.printStackTrace();
        }
    }

    public static void export3NFCSV(String url, String user, String password) {

        String path = "/home/cuprumtan/Documents/";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {

            DSLContext context = DSL.using(conn);

            List<Departments> departments = context.selectFrom(DEPARTMENTS).fetchInto(Departments.class);
            writeDepartmentsToCSV(path, departments, "departments.csv");

            List<Doctors> doctors = context.selectFrom(DOCTORS).fetchInto(Doctors.class);
            writeDoctorsToCSV(path, doctors, "doctors.csv");

            List<Patients> patients = context.selectFrom(PATIENTS).fetchInto(Patients.class);
            writePatientsToCSV(path, patients, "patients.csv");

            List<QualificationCategories> qualificationCategories = context.selectFrom(QUALIFICATION_CATEGORIES).fetchInto(QualificationCategories.class);
            writeQualificationCategoriesToCSV(path, qualificationCategories, "qualification_categories.csv");

            List<SpecialDepartments> specialDepartments = context.selectFrom(SPECIAL_DEPARTMENTS).fetchInto(SpecialDepartments.class);
            writeSpecialDepartmentsToCSV(path, specialDepartments, "special_departments.csv");

            List<Visits> visits = context.selectFrom(VISITS).fetchInto(Visits.class);
            writeVisitsToCSV(path, visits, "visits.csv");

        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }
}
