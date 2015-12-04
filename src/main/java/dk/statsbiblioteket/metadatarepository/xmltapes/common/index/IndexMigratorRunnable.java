package dk.statsbiblioteket.metadatarepository.xmltapes.common.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.sqlindex.SQLIndex;
import redis.clients.jedis.JedisPoolConfig;

public class IndexMigratorRunnable {

    private final static String SQLINDEX_DRIVER_PROP="dk.statsbiblioteket.xmltapes.migrator.sqlindex.driver";
    private final static String SQLINDEX_JDBCURL_PROP="dk.statsbiblioteket.xmltapes.migrator.sqlindex.jdbcurl";
    private final static String SQLINDEX_DBUSER_PROP="dk.statsbiblioteket.xmltapes.migrator.sqlindex.dbuser";
    private final static String SQLINDEX_DBPASS_PROP="dk.statsbiblioteket.xmltapes.migrator.sqlindex.dbpass";
    
    private final static String REDIS_HOST_PROP="dk.statsbiblioteket.xmltapes.migrator.redis.host";
    private final static String REDIS_PORT_PROP="dk.statsbiblioteket.xmltapes.migrator.redis.port";
    private final static String REDIS_DATABASE_PROP="dk.statsbiblioteket.xmltapes.migrator.redis.database";
    
    private final static String POSTGRES_TO_REDIS = "postgres-to-redis";
    private final static String REDIS_TO_POSTGRES = "redis-to-postgres";
    
    public static void main(String[] args) {
        if(args.length != 2) {
            System.err.println("Two arguments in the following order are required: properties-file method name");
            System.err.println("Method should be either '" + REDIS_TO_POSTGRES + "' or '" + POSTGRES_TO_REDIS + "'");
            System.exit(1);
        }
        String properties = args[0];
        File propertiesFile = new File(properties);
        if(!propertiesFile.exists()) {
            System.err.println("Failed to verify the existence of file properties file '" + propertiesFile.toString() + "'");
            System.exit(2);
        }
        
        String method = args[1];
        if(!method.equals(POSTGRES_TO_REDIS) && !method.equals(REDIS_TO_POSTGRES)) {
            System.err.println("Method was: '" + method + "', but must be either: '" 
                    + POSTGRES_TO_REDIS + "' or '" + REDIS_TO_POSTGRES + "'");
            System.exit(3);
        }

        IndexMigratorRunnable imr = new IndexMigratorRunnable(propertiesFile, method);
    }
    
    public IndexMigratorRunnable(File propertiesFile, String method) {
        try {
            Properties properties = new Properties();
            BufferedReader propertiesReader = new BufferedReader(new FileReader(propertiesFile));
            properties.load(propertiesReader);
            
            Index sqlIndex = getSQLIndex(properties);
            Index redisIndex = getRedisIndex(properties);
            
            IndexMigrator migrator;            
            if(method.equals(REDIS_TO_POSTGRES)) {
                migrator = new IndexMigrator(redisIndex, sqlIndex);
            } else {
                migrator = new IndexMigrator(sqlIndex, redisIndex);
            }
            migrator.migrate();
            
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(4);
        }
        
        
    }
    
    private Index getSQLIndex(Properties properties) {
        String driver = properties.getProperty(SQLINDEX_DRIVER_PROP);
        String jdbcUrl = properties.getProperty(SQLINDEX_JDBCURL_PROP);
        String dbuser = properties.getProperty(SQLINDEX_DBUSER_PROP);
        String dbpass = properties.getProperty(SQLINDEX_DBPASS_PROP);
        SQLIndex index = new SQLIndex(driver, jdbcUrl, dbuser, dbpass);
        return index;
    }
    
    private Index getRedisIndex(Properties properties) {
        String host = properties.getProperty(REDIS_HOST_PROP);
        int port = Integer.parseInt(properties.getProperty(REDIS_PORT_PROP));
        int database = Integer.parseInt(properties.getProperty(REDIS_DATABASE_PROP));
        RedisIndex index = new RedisIndex(host, port, database, getJedisPoolConfig());
        return index;
    }
    
    /**
     * Get a default JedisPoolConfig 
     */
    private JedisPoolConfig getJedisPoolConfig() {
        JedisPoolConfig jpc = new JedisPoolConfig();
        jpc.setWhenExhaustedAction(org.apache.commons.pool.impl.GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
        jpc.setMaxActive(10);
        jpc.setMaxIdle(5);
        jpc.setMinIdle(1);
        jpc.setTestOnBorrow(true);
        jpc.setTestOnReturn(true);
        jpc.setTestWhileIdle(true);
        jpc.setNumTestsPerEvictionRun(10);
        jpc.setTimeBetweenEvictionRunsMillis(60000);
        jpc.setMaxWait(30000);
        return jpc;
    }

}
