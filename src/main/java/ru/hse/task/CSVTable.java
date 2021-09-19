package ru.hse.task;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVTable {
    private final List<String> columnNames = new ArrayList<>();
    private final List<List<String>> rows = new ArrayList<>();

    public CSVTable(String fileName) throws IOException, WrongCsvStructureException {
        Reader in = new FileReader(fileName);
        CSVParser parser = CSVFormat.DEFAULT.
                builder().
                setHeader().
                setSkipHeaderRecord(true).
                build().
                parse(in);
        if (parser.getHeaderNames().isEmpty()) {
            throw new WrongCsvStructureException("Empty csv file");
        }
        columnNames.addAll(parser.getHeaderNames());
        for (CSVRecord record : parser) {
            rows.add(parseRow(record));
        }
    }

    public void createTable(String tableName, Connection connection) throws SQLException {
        List<String> columnNamesWithType = columnNames.stream().
                map(name -> name + " varchar(10)").
                collect(Collectors.toList());
        String sql = String.format(
                "create table %s (%s)",
                tableName,
                String.join(",", columnNamesWithType)
        );
        connection.prepareStatement(sql).execute();
    }

    public int insertAllValues(String tableName, Connection connection) throws SQLException {
        List<String> formattedValues = rows.stream().
                map(row -> "(" + String.join(",", row) + ")").
                collect(Collectors.toList());
        String sql = String.format(
                "insert into %s values %s",
                tableName,
                String.join(",", formattedValues)
        );
        return connection.prepareStatement(sql).executeUpdate();
    }

    private List<String> parseRow(CSVRecord record) throws WrongCsvStructureException {
        List<String> row = new ArrayList<>();
        for (String columnName : columnNames) {
            if (!record.isSet(columnName)) {
                throw new WrongCsvStructureException(
                        String.format("Missing value for column '%s' in %d row", columnName, record.getRecordNumber())
                );
            }
            row.add(record.get(columnName));
        }
        return row;
    }
}
