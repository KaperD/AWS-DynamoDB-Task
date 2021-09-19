package ru.hse.task;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
    private final String name;

    public CSVTable(String fileName, String name) throws IOException, WrongCsvStructureException {
        this(new FileInputStream(fileName), name);
    }

    public CSVTable(InputStream inputStream, String name) throws IOException, WrongCsvStructureException {
        this.name = name;
        try {
            Reader in = new InputStreamReader(inputStream);
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
        } catch (IllegalArgumentException e) {
            throw new WrongCsvStructureException(e);
        }
    }

    public String getName() {
        return name;
    }

    public void createTable(Connection connection) throws SQLException {
        List<String> columnNamesWithType = columnNames.stream().
                map(name -> name + " varchar(10)").
                collect(Collectors.toList());
        String sql = String.format(
                "create table %s (%s)",
                name,
                String.join(",", columnNamesWithType)
        );
        connection.prepareStatement(sql).execute();
    }

    public int insertAllValues(Connection connection) throws SQLException {
        if (rows.isEmpty()) {
            return 0;
        }
        List<String> formattedValues = rows.stream().
                map(row -> row.stream().map(val -> "'" + val + "'").collect(Collectors.toList())).
                map(row -> "(" + String.join(",", row) + ")").
                collect(Collectors.toList());
        String sql = String.format(
                "insert into %s values %s",
                name,
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
