package dk.statsbiblioteket.metadatarepository.xmltapes.common.index;

import java.io.File;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 5/23/13
 * Time: 6:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class Entry {


    public static final String SEPARATOR_CHAR = "@";
    private final File tape;
    private final long offset;

    public Entry(final File tape, final long offset) {
        this.tape = tape;
        this.offset = offset;
    }

    public String serialize() {
        return tape.toString() + SEPARATOR_CHAR + offset;
    }

    public static Entry deserialize(String file) {
        String[] splits = file.split(Pattern.quote(SEPARATOR_CHAR));
        File tapeFile = new File(splits[0]);
        long offset = Long.parseLong(splits[1]);
        return new Entry(tapeFile, offset);
    }


    public File getTape() {
        return tape;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "Entry{" +
               "tape=" + tape +
               ", offset=" + offset +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Entry)) return false;

        Entry entry = (Entry) o;

        if (getOffset() != entry.getOffset()) return false;
        return getTape().equals(entry.getTape());

    }

    @Override
    public int hashCode() {
        int result = getTape().hashCode();
        result = 31 * result + (int) (getOffset() ^ (getOffset() >>> 32));
        return result;
    }
}
