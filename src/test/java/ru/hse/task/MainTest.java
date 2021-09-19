package ru.hse.task;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MainTest {
    private InMemoryServer server;

    @BeforeEach
    void setServer() throws SQLException {
        server = new InMemoryServer();
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    @Test
    void testCorrectCsv() throws WrongCsvStructureException, IOException, SQLException {
        List<List<String>> expected = List.of(
                List.of("a", "b", "c"),
                List.of("d", "e", "f")
        );
        testOk("testCorrectCsv", expected);
    }

    @Test
    void testCorrectCsvWithExtraData() throws WrongCsvStructureException, IOException, SQLException {
        List<List<String>> expected = List.of(
                List.of("a", "b", "c"),
                List.of("d", "e", "f")
        );
        testOk("testCorrectCsvWithExtraData", expected);
    }

    @Test
    void testCorrectCsvEmptyElements() throws WrongCsvStructureException, IOException, SQLException {
        List<List<String>> expected = List.of(
                List.of("a", "", "c"),
                List.of("d", "e", "")
        );
        testOk("testCorrectCsvEmptyElements", expected);
    }

    @Test
    void testCorrectEmptyCsv() throws WrongCsvStructureException, IOException, SQLException {
        List<List<String>> expected = Collections.emptyList();
        testOk("testCorrectEmptyCsv", expected);
    }

    @Test
    void testWrongCsvMissingHeader() {
        testWrong("testWrongCsvMissingHeader", WrongCsvStructureException.class);
    }

    @Test
    void testWrongCsvMissingValue() {
        testWrong("testWrongCsvMissingValue", WrongCsvStructureException.class);
    }

    @Test
    void testWrongEmptyCsv() {
        testWrong("testWrongEmptyCsv", WrongCsvStructureException.class);
    }

    @Test
    void testSelect() throws SQLException, WrongCsvStructureException, IOException {
        CSVTable table = new CSVTable(getResource("testSelect.csv"), "testSelect");
        server.saveTable(table);
        List<List<String>> expected = List.of(
                List.of("apple", "dog", "cat"),
                List.of("apple", "egg", "dog")
        );
        List<List<String>> actual = server.read("select * from testSelect where first = 'apple'");
        assertEquals(expected, actual);
    }

    private void testOk(String testName, List<List<String>> expected) throws WrongCsvStructureException, IOException,
            SQLException {
        CSVTable table = new CSVTable(getResource(testName + ".csv"), testName);
        server.saveTable(table);
        List<List<String>> actual = server.readTable(testName);
        assertEquals(expected, actual);
    }

    private void testWrong(String testName, Class<? extends Throwable> expectedExceptionType) {
        Assertions.assertThrows(expectedExceptionType, () -> {
            CSVTable table = new CSVTable(getResource(testName + ".csv"), testName);
            server.saveTable(table);
            List<List<String>> actual = server.readTable(testName);
        });
    }

    private InputStream getResource(String path) {
        return this.getClass().getResourceAsStream(path);
    }
}