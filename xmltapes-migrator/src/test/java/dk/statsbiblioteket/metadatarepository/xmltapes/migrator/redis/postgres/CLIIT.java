package dk.statsbiblioteket.metadatarepository.xmltapes.migrator.redis.postgres;

import com.google.common.collect.ImmutableMap;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Entry;
import dk.statsbiblioteket.metadatarepository.xmltapes.common.index.Index;
import dk.statsbiblioteket.metadatarepository.xmltapes.migrator.IntegrationTestImmediateReporter;
import dk.statsbiblioteket.metadatarepository.xmltapes.redis.RedisIndex;
import dk.statsbiblioteket.metadatarepository.xmltapes.sqlindex.SQLIndex;
import dk.statsbiblioteket.util.Projects;
import dk.statsbiblioteket.util.console.ProcessRunner;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

/**
 * Command Line Interface Integration Test
 */
@Listeners(IntegrationTestImmediateReporter.class)
public class CLIIT {

    @Test
    public void testRedisToPostgres() throws Exception {

        String versionNumber = getVersion();
        File target = getTargetDirectory();


        //Extract the tool
        ProcessRunner extractor = new ProcessRunner("bash","-c","tar -xzf xmltapes-migrator-"+versionNumber+"-bundle.tar.gz");
        extractor.setStartingDir(target);
        extractor.run();
        assertEquals(extractor.getReturnCode(),0,extractor.getProcessErrorAsString());

        //Index config params
        String redisHost = "localhost";
        int redisPort = 16379;
        int redisDB = 5;
        String sqlDriver = "org.postgresql.Driver";
        String jdbcUrl = "jdbc:postgresql://localhost:15432/tapes";
        String dbUser = "docker";
        String dbPass = "docker";


        //Write the right config to the config file
        writeConfigFile(versionNumber, target, redisHost, redisPort, redisDB, sqlDriver, jdbcUrl, dbUser, dbPass);

        //populate the indexes
        String tape1 = "tape7";
        String tape2 = "tape8";
        String tape3 = "tape9";
        String tape4 = "tape10";

        //The Entries
        URI blob1ID = URI.create("blob100");
        Entry entry1 = new Entry(new File(tape2), 0);
        URI blob2ID = URI.create("blob200");
        Entry entry2 = new Entry(new File(tape2), 10045);
        URI blob3ID = URI.create("blob300");
        Entry entry3 = new Entry(new File(tape3), 0);

        //This config should match the config file above
        Index postgres = new SQLIndex(sqlDriver, jdbcUrl, dbUser, dbUser);
        postgres.clear();
        Index redis = new RedisIndex(redisHost, redisPort, redisDB, new JedisPoolConfig());
        redis.clear();

        populateIndex(asSet(tape1, tape2, tape3),ImmutableMap.of(blob1ID,entry1,blob2ID,entry2,blob3ID,entry3), redis);

        //Run the migration
        ProcessRunner migrator = new ProcessRunner("bash","-c","bin/migrate.sh $PWD/config/ redis-to-postgres");
        migrator.setStartingDir(new File(getTargetDirectory(),"xmltapes-migrator-"+getVersion()));
        migrator.run();
        assertEquals(migrator.getReturnCode(),0,migrator.getProcessErrorAsString());


        assertMigrationOK(asSet(tape1, tape2, tape3),ImmutableMap.of(blob1ID,entry1,blob2ID,entry2,blob3ID,entry3), postgres);



    }

    @SafeVarargs
    private final <T> Set<T> asSet(T... tape) {
        return new HashSet<>(Arrays.asList(tape));
    }

    protected void populateIndex(Set<String> tapes, Map<URI,Entry> entries, Index index) {
        for (Map.Entry<URI, Entry> uriEntryEntry : entries.entrySet()) {
           index.addLocation(uriEntryEntry.getKey(),uriEntryEntry.getValue());
        }
        for (String tape : tapes) {
            index.setIndexed(tape);
        }
    }

    protected void assertMigrationOK(Set<String> tapes, Map<URI,Entry> entries, Index index) {
        for (Map.Entry<URI, Entry> uriEntryEntry : entries.entrySet()) {
            assertEquals(index.getLocation(uriEntryEntry.getKey()), uriEntryEntry.getValue());
        }
        for (String tape : tapes) {
            assertEquals(index.isIndexed(tape), true);
        }

        Set<URI> keys = new HashSet<>(entries.keySet());
        Iterator<URI> uriIterator = index.listIds(null);
        while (uriIterator.hasNext()) {
            URI next = uriIterator.next();
            assertTrue(keys.remove(next));
        }
        assertTrue(keys.isEmpty());

        Set<String> tapesTemp = new HashSet<>(tapes);
        Iterator<String> tapeIterator = index.listIndexedTapes();
        while (tapeIterator.hasNext()) {
            String next = tapeIterator.next();
            assertTrue(tapesTemp.remove(next));
        }
        assertTrue(tapesTemp.isEmpty());
    }

    protected void writeConfigFile(String versionNumber, File target, String redisHost, int redisPort, int redisDB, String sqlDriver, String jdbcUrl, String dbUser, String dbPass) throws IOException {
        BufferedWriter configFile = new BufferedWriter(
                new FileWriter(new File(target, "xmltapes-migrator-" + versionNumber + "/config/migrator.properties")));

        configFile.write("dk.statsbiblioteket.xmltapes.migrator.sqlindex.driver=" + sqlDriver + "\n" +
                         "dk.statsbiblioteket.xmltapes.migrator.sqlindex.jdbcurl=" + jdbcUrl + "\n" +
                         "dk.statsbiblioteket.xmltapes.migrator.sqlindex.dbuser=" + dbUser + "\n" +
                         "dk.statsbiblioteket.xmltapes.migrator.sqlindex.dbpass=" + dbPass + "\n" +
                         "\n" +
                         "dk.statsbiblioteket.xmltapes.migrator.redis.host=" + redisHost + "\n" +
                         "dk.statsbiblioteket.xmltapes.migrator.redis.port=" + redisPort + "\n" +
                         "dk.statsbiblioteket.xmltapes.migrator.redis.database=" + redisDB);
        configFile.close();
    }

    protected File getTargetDirectory() {
        File root = Projects.getProjectRoot(this.getClass());
        return new File(root,"target");
    }

    protected String getVersion() throws IOException {
        Properties properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("version.properties"));
        return properties.getProperty("project.version");
    }
}
