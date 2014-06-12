package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import java.net.URI;

public class XmlTapesConfig {

    private URI tempDir;
    private URI cachingDir;
    private URI tapingDir;
    private URI tapeDir;

    private long tapeWaitBeforeStartInMS;

    private long minAgeToTapeInMS;

    private long tapeSize;

    private String tapeExtension, tapePrefix, tempTapePrefix;
    private String redisHost;
    private int redisPort;
    private int redisDB;
    private boolean fixErrorsOnRestart;
    private boolean rebuildIndexFromTapesOnRestart;

    public URI getCachingDir() {
        return cachingDir;
    }

    public void setCachingDir(URI cachingDir) {
        this.cachingDir = cachingDir;
    }

    public URI getTempDir() {
        return tempDir;
    }

    public void setTempDir(URI tempDir) {
        this.tempDir = tempDir;
    }

    public URI getTapingDir() {
        return tapingDir;
    }

    public void setTapingDir(URI tapingDir) {
        this.tapingDir = tapingDir;
    }

    public long getTapeWaitBeforeStartInMS() {
        return tapeWaitBeforeStartInMS;
    }

    public void setTapeWaitBeforeStartInMS(long tapeWaitBeforeStartInMS) {
        this.tapeWaitBeforeStartInMS = tapeWaitBeforeStartInMS;
    }

    public long getMinAgeToTapeInMS() {
        return minAgeToTapeInMS;
    }

    public void setMinAgeToTapeInMS(long minAgeToTapeInMS) {
        this.minAgeToTapeInMS = minAgeToTapeInMS;
    }

    public long getTapeSize() {
        return tapeSize;
    }

    public void setTapeSize(long tapeSize) {
        this.tapeSize = tapeSize;
    }

    public String getTapeExtension() {
        return tapeExtension;
    }

    public void setTapeExtension(String tapeExtension) {
        this.tapeExtension = tapeExtension;
    }

    public String getTapePrefix() {
        return tapePrefix;
    }

    public void setTapePrefix(String tapePrefix) {
        this.tapePrefix = tapePrefix;
    }

    public String getTempTapePrefix() {
        return tempTapePrefix;
    }

    public void setTempTapePrefix(String tempTapePrefix) {
        this.tempTapePrefix = tempTapePrefix;
    }

    public URI getTapeDir() {
        return tapeDir;
    }

    public void setTapeDir(URI tapeDir) {
        this.tapeDir = tapeDir;
    }

    public String getRedisHost() {
        return redisHost;
    }



    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }



    public boolean isFixErrorsOnRestart() {
        return fixErrorsOnRestart;
    }

    public void setFixErrorsOnRestart(boolean fixErrorsOnRestart) {
        this.fixErrorsOnRestart = fixErrorsOnRestart;
    }

    public boolean isRebuildIndexFromTapesOnRestart() {
        return rebuildIndexFromTapesOnRestart;
    }

    public void setRebuildIndexFromTapesOnRestart(boolean rebuildIndexFromTapesOnRestart) {
        this.rebuildIndexFromTapesOnRestart = rebuildIndexFromTapesOnRestart;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public int getRedisDB() {
        return redisDB;
    }

    public void setRedisDB(int redisDB) {
        this.redisDB = redisDB;
    }
}
