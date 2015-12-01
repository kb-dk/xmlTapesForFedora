package dk.statsbiblioteket.metadatarepository.xmltapes.junit;

import dk.statsbiblioteket.metadatarepository.xmltapes.postgres.PostgresIndexConfig;

public class PostgresTestSettings {
    public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
    public static final String JDBC_URL = "jdbc:postgresql:tapes";
    public static final String POSTGRES_USER = "ktc";
    public static final String POSTGRES_PASS = "foobar";
    
    
    public static PostgresIndexConfig getTestConfig() {
        return new PostgresIndexConfig(POSTGRES_DRIVER, JDBC_URL, POSTGRES_USER, POSTGRES_PASS);
    }
}
