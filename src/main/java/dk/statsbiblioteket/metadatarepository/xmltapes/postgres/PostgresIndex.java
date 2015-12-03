package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;


/**
 * Postgres backed index for XMLTapes 
 */
public class PostgresIndex implements Index {

    private static final Logger log = LoggerFactory.getLogger(PostgresIndex.class);
    
    private ComboPooledDataSource connectionPool = new ComboPooledDataSource();
    
    public PostgresIndex(String dbDriver, String jdbcUrl, String dbUser, String dbPass) {
        try {
            connectionPool.setDriverClass(dbDriver);
            connectionPool.setJdbcUrl(jdbcUrl);
            connectionPool.setUser(dbUser);
            connectionPool.setPassword(dbPass);
        } catch (Exception e) {
            throw new IllegalStateException("Could not connect to the database '" +  jdbcUrl + "'", e);
        }        
    }

    @Override
    public Entry getLocation(URI id) {
        String getLocationSql = "SELECT tapename, tapeoffset FROM storeIndex WHERE id = ?";
        
        try(Connection conn = connectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(getLocationSql);) {
            ps.setString(1, id.toString());
            try(ResultSet rs = ps.executeQuery()) {
                if(!rs.next()) {
                    log.debug("Requested id '{}' but it was not found in the index", id);
                    return null;
                } else {
                    String name = rs.getString("tapename");
                    Long offset = rs.getLong("tapeoffset");
                    Entry entry = new Entry(new File(name), offset);
                    return entry;
                }
            }
        } catch (SQLException e) {
            log.error("Caught exception while interacting with database", e);
            throw new RuntimeException("Problem communicating with database", e);
        }
    }

    @Override
    public void addLocation(URI id, Entry location) {
        String addLocationSql = "INSERT INTO storeIndex (id, tapename, tapeoffset) (SELECT ?, ?, ?"
                + " WHERE NOT EXISTS (SELECT id FROM storeIndex WHERE id = ?))";
        String updateLocationSql = "UPDATE storeIndex SET tapename = ?, tapeoffset = ?"
                + " WHERE id = ?";

        try(Connection conn = connectionPool.getConnection();
            PreparedStatement addPs = conn.prepareStatement(addLocationSql);
            PreparedStatement updatePs = conn.prepareStatement(updateLocationSql); ) {
            
            addPs.setString(1, id.toString());
            addPs.setString(2, location.getTape().toString());
            addPs.setLong(3, location.getOffset());
            addPs.setString(4, id.toString());
            
            updatePs.setString(1, location.getTape().toString());
            updatePs.setLong(2, location.getOffset());
            updatePs.setString(3, id.toString());
            
            updatePs.execute();
            addPs.execute();
        } catch (SQLException e) {
            log.error("Caught exception while interacting with database", e);
            throw new RuntimeException("Problem communicating with database", e);
        }

    }

    @Override
    public void remove(URI id) {
        String removeSql = "DELETE FROM storeIndex WHERE id = ?";
        
        try(Connection conn = connectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(removeSql);) {
        
            ps.setString(1, id.toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Caught exception while interacting with database", e);
            throw new RuntimeException("Problem communicating with database", e);
        }
    }

    @Override
    public Iterator<URI> listIds(String filterPrefix) {
        String selectSql;
        if(filterPrefix == null) {
            selectSql = "SELECT id FROM storeIndex";    
        } else {
            selectSql = "SELECT id FROM storeIndex WHERE id like ?";
        }
        
        try {
            Connection conn = connectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(selectSql);
            if(filterPrefix != null) { 
                ps.setString(1, filterPrefix + "%");
            }
            
            Iterator<URI> iterator = new PostgresIterator(ps);
            return iterator;
        } catch (SQLException e) {
            log.error("Failed obtaining an iterator", e);
            throw new RuntimeException("Failed obtaining an iterator for filterPrefix '" + filterPrefix + "'.", e);
        }
    }

    @Override
    public boolean isIndexed(String tapename) {
        String selectIndexedSql = "SELECT tapename FROM indexed where tapename = ?";
        
        try(Connection conn = connectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(selectIndexedSql);) {

            ps.setString(1, tapename.toString());
            try(ResultSet rs = ps.executeQuery()) {
                if(!rs.next()) {
                    return false;
                } else {
                    String name = rs.getString("tapename");
                    return (name == tapename); 
                }
            }
        } catch (SQLException e) {
            log.error("Caught exception while interacting with database", e);
            throw new RuntimeException("Problem communicating with database", e);
        }
    }

    @Override
    public void setIndexed(String tapename) {
        String setIndexedSql = "INSERT INTO indexed (tapename) (SELECT ?"
                + " WHERE NOT EXISTS (SELECT tapename FROM indexed WHERE tapename = ?))";
        String updateIndexedSql = "UPDATE indexed SET tapename = ? WHERE tapename = ?";

        try(Connection conn = connectionPool.getConnection();
            PreparedStatement setPs = conn.prepareStatement(setIndexedSql);
            PreparedStatement updatePs = conn.prepareStatement(updateIndexedSql); ) {
            
            setPs.setString(1, tapename);
            setPs.setString(2, tapename);
            
            updatePs.setString(1, tapename);
            updatePs.setString(2, tapename);
            
            updatePs.execute();
            setPs.execute();
        } catch (SQLException e) {
            log.error("Caught exception while interacting with database", e);
            throw new RuntimeException("Problem communicating with database", e);
        }

    }

    @Override
    public void clear() {
        String clearIndexSql = "TRUNCATE storeIndex, indexed";
        
        try(Connection conn = connectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(clearIndexSql);) {
            
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Caught exception while interacting with database", e);
            throw new RuntimeException("Problem communicating with database", e);
        }
    }

}
