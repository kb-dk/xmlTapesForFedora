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
    private File tape;
    private long offset;

    public Entry(File tape, long offset) {
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
}
