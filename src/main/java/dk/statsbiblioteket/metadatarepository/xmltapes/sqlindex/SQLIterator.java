package dk.statsbiblioteket.metadatarepository.xmltapes.sqlindex;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLIterator implements Iterator<URI>, AutoCloseable {
    
    private static final Logger log = LoggerFactory.getLogger(SQLIterator.class);

    private final ResultSet rs;
    private URI currentValue = null;
            
    public SQLIterator(PreparedStatement ps) throws SQLException {
        Connection conn = ps.getConnection();
        // Set autocommit to false, to allow setting fetch size
        conn.setAutoCommit(false);
        ps.setFetchSize(100);
        
        long tStart = System.currentTimeMillis();
        log.debug("Executing query to get resultset");
        rs = ps.executeQuery();
        log.debug("Finished executing issues query, it took: {} ms", (System.currentTimeMillis() - tStart));
    }
    
    public void close() throws SQLException {
        Statement s = rs.getStatement();
        Connection conn = s.getConnection();
        conn.setAutoCommit(true);
        DbUtils.closeQuietly(conn, s, rs);
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
                currentValue = URI.create(rs.getString(1));
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

        URI result = currentValue;
        currentValue = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void finalize() {
        try {
            close();
        } catch (SQLException e) {
            log.error("Failed to clean up.", e);
        }
    }
}
