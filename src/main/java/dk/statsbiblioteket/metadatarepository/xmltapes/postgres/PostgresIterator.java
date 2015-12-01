package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresIterator implements Iterator<URI> {
    
    private static final Logger log = LoggerFactory.getLogger(PostgresIterator.class);

    private final PreparedStatement ps;
    private ResultSet rs = null;
    private Connection conn = null;
    private String currentValue = null;
            
    public PostgresIterator(PreparedStatement ps) throws SQLException {
        this.ps = ps;
        initialize();
    }

    private void initialize() throws SQLException {
        log.debug("Starting initialization");
        
        conn = ps.getConnection();
        conn.setAutoCommit(false);
        ps.setFetchSize(100);
        long tStart = System.currentTimeMillis();
        log.debug("Executing query to get resultset");
        rs = ps.executeQuery();
        log.debug("Finished executing issues query, it took: {} ms", (System.currentTimeMillis() - tStart));
    }
    
    public void close() {
        try {
            if(rs != null) {
               rs.close();
               rs = null;
            }
            
            if(ps != null) {
                ps.close();
            }
            
            if(conn != null) {
                conn.setAutoCommit(true);
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            log.error("Failed to close iterator", e);
        }
    }

    @Override
    public boolean hasNext() {
        if(currentValue != null) {
            return true;
        }
        
        if(rs == null) {
            return false;
        }
        
        try {
            if(rs.next()) {
                currentValue = rs.getString(1);
                return true;
            } else {
                close();
                return false;
            }
        } catch (SQLException e) {
            log.error("Failure while loading next result", e);
            throw new RuntimeException("Failed while loading result", e);
        }
    }

    @Override
    public URI next() {
        if (!hasNext() || currentValue == null){
            throw new NoSuchElementException();
        }

        URI result = URI.create(currentValue);
        currentValue = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
