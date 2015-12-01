package dk.statsbiblioteket.metadatarepository.xmltapes.postgres;

public class PostgresIndexConfig {

    private String databaseDriver;
    private String jdbcUrl;
    private String user;
    private String password;
    
    public PostgresIndexConfig() {}
    
    public PostgresIndexConfig(String driver, String jdbcUrl, String user, String pass) {
        this.databaseDriver = driver;
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = pass;
    }
    
    public String getDatabaseDriver() {
        return databaseDriver;
    }
    
    public void setDatabaseDriver(String databaseDriver) {
        this.databaseDriver = databaseDriver;
    }
    
    public String getJdbcUrl() {
        return jdbcUrl;
    }
    
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
    
    public String getUser() {
        return user;
    }
    
    public void setUser(String user) {
        this.user = user;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    @Override
    public String toString() {
        return "PostgresIndexConfig [databaseDriver=" + databaseDriver + ", jdbcUrl=" + jdbcUrl + ", user=" + user + "]";
    }
}
