package ru.hse.task;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.hsqldb.server.Server;

public class InMemoryServer implements AutoCloseable {
    private final Connection connection;
    private final Server server = new Server();

    public InMemoryServer() throws SQLException {
        server.setSilent(true);
        server.setLogWriter(null);
        server.setDatabaseName(0, "mainDb");
        server.setDatabasePath(0, "mem:mainDb");
        server.start();
        connection = DriverManager.getConnection("jdbc:hsqldb:mem:mainDb", "SA", "");
    }

    public void saveTable(CSVTable table) throws SQLException {
        table.createTable(connection);
        table.insertAllValues(connection);
    }

    public List<List<String>> readTable(String tableName) throws SQLException {
        return read("select * from " + tableName);
    }

    public List<List<String>> read(String selectSql) throws SQLException {
        ResultSet resultSet = connection.prepareStatement(selectSql).executeQuery();
        int columnCount = resultSet.getMetaData().getColumnCount();
        List<List<String>> rows = new ArrayList<>();
        while (resultSet.next()) {
            List<String> row = new ArrayList<>();
            for (int k = 1; k <= columnCount; k++) {
                row.add(resultSet.getString(k));
            }
            rows.add(row);
        }
        return rows;
    }

    @Override
    public void close() {
        server.stop();
    }
}
