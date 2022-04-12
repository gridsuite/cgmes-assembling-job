package org.gridsuite.cgmes.assembling.job;

public final class JdbcQueries {

    private JdbcQueries() {
    }

    public static final String INSERT_HANDLED_FILE = "INSERT INTO handled_files (filename, origin, handled_date, uuid) VALUES(?, ?, ?, ?)";
    public static final String INSERT_IMPORTED_FILE = "INSERT INTO imported_files (filename, origin, import_date) VALUES(?, ?, ?)";
    public static final String INSERT_DEPENDENCIES = "INSERT INTO handled_files_dependencies (uuid, dependency_uuid) VALUES(?, ?)";
    public static final String SELECT_HANDLED_FILE = "SELECT (filename, origin, handled_date) FROM handled_files where filename = ? and origin = ?";
    public static final String SELECT_IMPORTED_FILE = "SELECT (filename, origin, import_date) FROM imported_files where filename = ? and origin = ?";
    public static final String SELECT_FILENAME_BY_UUID = "SELECT (filename) FROM handled_files where uuid = ? and origin = ?";
    public static final String SELECT_UUID_BY_FILENAME = "SELECT (uuid) FROM handled_files where filename = ? and origin = ?";
    public static final String SELECT_DEPENDENCIES = "SELECT dependency_uuid FROM handled_files_dependencies where uuid = ?";
}
