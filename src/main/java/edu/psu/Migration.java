package edu.psu;

import org.jooq.*;
import org.jooq.impl.DSL;

import java.sql.*;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static edu.psu.generateSQLite.tables.PermCityPolyclinic_7Registry.PERM_CITY_POLYCLINIC_7_REGISTRY;
import static edu.psu.generatePostgreSQL.tables.Departments.DEPARTMENTS;
import static edu.psu.generatePostgreSQL.tables.Doctors.DOCTORS;
import static edu.psu.generatePostgreSQL.tables.Patients.PATIENTS;
import static edu.psu.generatePostgreSQL.tables.QualificationCategories.QUALIFICATION_CATEGORIES;
import static edu.psu.generatePostgreSQL.tables.SpecialDepartments.SPECIAL_DEPARTMENTS;
import static edu.psu.generatePostgreSQL.tables.Visits.VISITS;

public class Migration {

    public static void insertQualificationGategories(String urlSQLite, String urlPostgreSQL, String userPostgreSQL, String passwordPostgreSQL)
    {
        try (Connection conn = DriverManager.getConnection(urlSQLite)) {
            DSLContext context = DSL.using(conn);
            List<String> qualificationCategories =
                    context.selectDistinct(PERM_CITY_POLYCLINIC_7_REGISTRY.QUALIFICATION_CATEGORY_DOCTOR)
                            .from(PERM_CITY_POLYCLINIC_7_REGISTRY)
                            .fetch()
                            .getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.QUALIFICATION_CATEGORY_DOCTOR);

            try (Connection pgConn = DriverManager.getConnection(urlPostgreSQL, userPostgreSQL, passwordPostgreSQL)) {
                DSLContext pgContext = DSL.using(pgConn);

                for (int i=0; i < qualificationCategories.size(); i++)
                {
                    pgContext.insertInto(QUALIFICATION_CATEGORIES,
                            QUALIFICATION_CATEGORIES.QUALIFICATION_CATEGORY)
                            .values(qualificationCategories.get(i))
                            .execute();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                System.exit(0);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public static void insertDepartments(String urlSQLite, String urlPostgreSQL, String userPostgreSQL, String passwordPostgreSQL)
    {
        try (Connection conn = DriverManager.getConnection(urlSQLite)) {
            DSLContext context = DSL.using(conn);
            List<String> departments =
                    context.selectDistinct(PERM_CITY_POLYCLINIC_7_REGISTRY.DEPARTMENT)
                            .from(PERM_CITY_POLYCLINIC_7_REGISTRY)
                            .fetch()
                            .getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.DEPARTMENT);

            try (Connection pgConn = DriverManager.getConnection(urlPostgreSQL, userPostgreSQL, passwordPostgreSQL)) {
                DSLContext pgContext = DSL.using(pgConn);

                for (int i=0; i < departments.size(); i++)
                {
                    pgContext.insertInto(DEPARTMENTS,
                            DEPARTMENTS.DEPARTMENT)
                            .values(departments.get(i))
                            .execute();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                System.exit(0);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public static void insertSpecialDepartments(String urlSQLite, String urlPostgreSQL, String userPostgreSQL, String passwordPostgreSQL)
    {
        try (Connection conn = DriverManager.getConnection(urlSQLite)) {
            DSLContext context = DSL.using(conn);
            Map<String, String> specialDepartments =
                    context.selectDistinct(PERM_CITY_POLYCLINIC_7_REGISTRY.DEPARTMENT, PERM_CITY_POLYCLINIC_7_REGISTRY.SPECIAL_DEPARTMENT)
                            .from(PERM_CITY_POLYCLINIC_7_REGISTRY)
                            .fetch()
                    .intoMap(PERM_CITY_POLYCLINIC_7_REGISTRY.SPECIAL_DEPARTMENT, PERM_CITY_POLYCLINIC_7_REGISTRY.DEPARTMENT);

            try (Connection pgConn = DriverManager.getConnection(urlPostgreSQL, userPostgreSQL, passwordPostgreSQL)) {
                DSLContext pgContext = DSL.using(pgConn);

                for (Map.Entry<String,String> entry : specialDepartments.entrySet())
                {
                    Integer id = pgContext.select(DEPARTMENTS.ID)
                            .from(DEPARTMENTS)
                            .where(DEPARTMENTS.DEPARTMENT.eq(entry.getValue()))
                            .fetch()
                            .getValues(DEPARTMENTS.ID, Integer.class).get(0);

                    pgContext.insertInto(SPECIAL_DEPARTMENTS,
                            SPECIAL_DEPARTMENTS.SPECIAL_DEPARTMENT, SPECIAL_DEPARTMENTS.ID_DEPARTMENT)
                            .values(entry.getKey(), id)
                            .execute();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                System.exit(0);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public static void insertDoctors(String urlSQLite, String urlPostgreSQL, String userPostgreSQL, String passwordPostgreSQL)
    {
        try (Connection conn = DriverManager.getConnection(urlSQLite)) {

            DSLContext context = DSL.using(conn);
            Result<Record9<Integer,String,String,String,String,String,String,String,String>> doctors =
                    context.selectDistinct(PERM_CITY_POLYCLINIC_7_REGISTRY.ID_DOCTOR,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.NAME_DOCTOR,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.BIRTH_DATE_DOCTOR,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.SEX_DOCTOR,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.EDUCATION_DOCTOR,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.POSITION_DOCTOR,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.QUALIFICATION_CATEGORY_DOCTOR,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.SPECIAL_DEPARTMENT,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.DEPARTMENT)
                            .from(PERM_CITY_POLYCLINIC_7_REGISTRY)
                            .fetch();

            try (Connection pgConn = DriverManager.getConnection(urlPostgreSQL, userPostgreSQL, passwordPostgreSQL)) {
                DSLContext pgContext = DSL.using(pgConn);

                for (int i=0; i < doctors.size(); i++)
                {
                    String[] name = doctors.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.NAME_DOCTOR).get(i).split("\\s+");

                    Integer idQualificationCategory = pgContext.select(QUALIFICATION_CATEGORIES.ID)
                            .from(QUALIFICATION_CATEGORIES)
                            .where(QUALIFICATION_CATEGORIES.QUALIFICATION_CATEGORY.eq(doctors
                                    .getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.QUALIFICATION_CATEGORY_DOCTOR).get(i)))
                            .fetch()
                            .getValues(QUALIFICATION_CATEGORIES.ID, Integer.class).get(0);

                    Integer idDepartment = pgContext.select(DEPARTMENTS.ID)
                            .from(DEPARTMENTS)
                            .where(DEPARTMENTS.DEPARTMENT.eq(doctors.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.DEPARTMENT).get(i)))
                            .fetch()
                            .getValues(DEPARTMENTS.ID, Integer.class).get(0);

                    Integer idSpecialDepartment = pgContext.select(SPECIAL_DEPARTMENTS.ID)
                            .from(SPECIAL_DEPARTMENTS)
                            .where(SPECIAL_DEPARTMENTS.SPECIAL_DEPARTMENT.eq(doctors
                                    .getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.SPECIAL_DEPARTMENT).get(i)))
                            .and(SPECIAL_DEPARTMENTS.ID_DEPARTMENT.eq(idDepartment))
                            .fetch()
                            .getValues(SPECIAL_DEPARTMENTS.ID, Integer.class).get(0);

                    pgContext.insertInto(DOCTORS)
                            .values(doctors.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.ID_DOCTOR).get(i),
                                    name[0], name[1], name[2],
                                    LocalDate.parse(doctors.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.BIRTH_DATE_DOCTOR).get(i)),
                                    doctors.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.SEX_DOCTOR).get(i),
                                    doctors.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.EDUCATION_DOCTOR).get(i),
                                    doctors.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.POSITION_DOCTOR).get(i),
                                    idQualificationCategory, idSpecialDepartment)
                            .execute();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                System.exit(0);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public static void insertPatients(String urlSQLite, String urlPostgreSQL, String userPostgreSQL, String passwordPostgreSQL)
    {
        try (Connection conn = DriverManager.getConnection(urlSQLite)) {
            DSLContext context = DSL.using(conn);
            Result<Record4<Integer,String,String,String>> patients =
                    context.selectDistinct(PERM_CITY_POLYCLINIC_7_REGISTRY.CARD_NUMBER,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.NAME_PATIENT,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.BIRTH_DATE_PATIENT,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.SEX_PATIENT)
                            .from(PERM_CITY_POLYCLINIC_7_REGISTRY)
                            .fetch();

            try (Connection pgConn = DriverManager.getConnection(urlPostgreSQL, userPostgreSQL, passwordPostgreSQL)) {
                DSLContext pgContext = DSL.using(pgConn);

                for (int i=0; i < patients.size(); i++)
                {
                    String[] name = patients.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.NAME_PATIENT).get(i).split("\\s+");

                    pgContext.insertInto(PATIENTS)
                            .values(patients.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.CARD_NUMBER).get(i),
                                    name[0], name[1], name[2],
                                    LocalDate.parse(patients.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.BIRTH_DATE_PATIENT).get(i)),
                                    patients.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.SEX_PATIENT).get(i))
                            .execute();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                System.exit(0);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }

    public static void insertVisits(String urlSQLite, String urlPostgreSQL, String userPostgreSQL, String passwordPostgreSQL)
    {
        try (Connection conn = DriverManager.getConnection(urlSQLite)) {

            DSLContext context = DSL.using(conn);
            Result<Record5<String,Integer,Integer,Integer,String>> visits =
                    context.selectDistinct(PERM_CITY_POLYCLINIC_7_REGISTRY.VISIT_DATETIME,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.ID_DOCTOR,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.CARD_NUMBER,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.VISIT_STATUS,
                            PERM_CITY_POLYCLINIC_7_REGISTRY.VISIT_COMMENT)
                            .from(PERM_CITY_POLYCLINIC_7_REGISTRY)
                            .fetch();

            try (Connection pgConn = DriverManager.getConnection(urlPostgreSQL, userPostgreSQL, passwordPostgreSQL)) {
                DSLContext pgContext = DSL.using(pgConn);

                for (int i=0; i < visits.size(); i++)
                {
                    pgContext.insertInto(VISITS)
                            .values(visits.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.VISIT_DATETIME).get(i),
                                    visits.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.ID_DOCTOR).get(i),
                                    visits.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.CARD_NUMBER).get(i),
                                    visits.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.VISIT_STATUS).get(i),
                                    visits.getValues(PERM_CITY_POLYCLINIC_7_REGISTRY.VISIT_COMMENT).get(i))
                            .execute();
                }
            }
            catch (SQLException e)
            {
                System.out.println(e.getMessage());
                System.exit(0);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
    }
    
    public static void migrateDataTo3NF(String urlSQLite, String urlPostgreSQL, String userPostgreSQL, String passwordPostgreSQL) {
        insertQualificationGategories(urlSQLite, urlPostgreSQL, userPostgreSQL, passwordPostgreSQL);
        insertDepartments(urlSQLite, urlPostgreSQL, userPostgreSQL, passwordPostgreSQL);
        insertSpecialDepartments(urlSQLite, urlPostgreSQL, userPostgreSQL, passwordPostgreSQL);
        insertDoctors(urlSQLite, urlPostgreSQL, userPostgreSQL, passwordPostgreSQL);
        insertPatients(urlSQLite, urlPostgreSQL, userPostgreSQL, passwordPostgreSQL);
        insertVisits(urlSQLite, urlPostgreSQL, userPostgreSQL, passwordPostgreSQL);
    }

}
