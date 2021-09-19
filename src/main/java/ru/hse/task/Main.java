package ru.hse.task;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.hsqldb.Server;

public class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Wrong number of arguments. You should specify only csv file path");
            System.exit(1);
        }
        String pathToCsv = args[0];
        CSVTable table = null;
        try {
            table = new CSVTable(pathToCsv);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        try (AutoClosableServer server = new AutoClosableServer()) {
            server.setDatabaseName(0, "mainDb");
            server.setDatabasePath(0, "mem:mainDb");
            server.start();
            try (Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:mainDb", "SA", "")) {
                table.createTable("test", connection);
                table.insertAllValues("test", connection);
                ResultSet resultSet = connection.prepareStatement("select * from test").executeQuery();
                int columnCount = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    for (int k = 1; k <= columnCount; k++) {
                        System.out.print(resultSet.getString(k));
                        if (k != columnCount) {
                            System.out.print(",");
                        }
                    }
                    System.out.println();
                }
            } catch (SQLException e) {
                System.out.println("Internal error: " + e.getMessage());
            }
        }
    }


    static class AutoClosableServer extends Server implements AutoCloseable {

        @Override
        public void close() {
            stop();
        }
    }
}
