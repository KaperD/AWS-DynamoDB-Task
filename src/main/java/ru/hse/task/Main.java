package ru.hse.task;

import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Wrong number of arguments. You should specify only csv file path");
            System.exit(1);
        }
        String pathToCsv = args[0];
        CSVTable table = null;
        try {
            table = new CSVTable(pathToCsv, "main");
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        try (InMemoryServer server = new InMemoryServer()) {
            server.saveTable(table);
            printTable(server.readTable(table.getName()));
            Scanner scanner = new Scanner(System.in);
            String sql = scanner.nextLine();
            while (!sql.equals("exit")) {
                if (!sql.isEmpty()) {
                    try {
                        printTable(server.read(sql));
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
                sql = scanner.nextLine();
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static void printTable(List<List<String>> rows) {
        rows.forEach(
                row -> System.out.println(String.join(",", row))
        );
    }
}
