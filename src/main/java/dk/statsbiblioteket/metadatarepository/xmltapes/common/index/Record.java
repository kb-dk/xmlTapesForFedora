package dk.statsbiblioteket.metadatarepository.xmltapes.common.index;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/7/13
 * Time: 10:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class Record {

    String id;

    long timestamp;

    public Record(String id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Record{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
