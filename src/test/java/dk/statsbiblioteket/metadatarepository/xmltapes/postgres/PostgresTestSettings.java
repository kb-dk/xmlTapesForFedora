package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.sqlindex.SQLIndex;

public class PostgresTestSettings {
    public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
    public static final String JDBC_URL = "jdbc:postgresql://localhost:15432/tapes";
    public static final String POSTGRES_USER = "docker";
    public static final String POSTGRES_PASS = "docker";
    
    
    public static Index getIndex() {
        return new SQLIndex(POSTGRES_DRIVER, JDBC_URL, POSTGRES_USER, POSTGRES_PASS);
    }
}
