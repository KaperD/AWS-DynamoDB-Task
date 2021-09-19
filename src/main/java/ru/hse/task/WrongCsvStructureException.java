package ru.hse.task;

public class WrongCsvStructureException extends Exception {

    public WrongCsvStructureException(String message) {
        super(message);
    }
}
