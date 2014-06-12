package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.CountingOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.kamranzafar.jtar.TarEntry;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Static utility methods to convert between index entries, fedora URIs and tar Entries and tar filenames
 * //TODO the rest of the methods
 */
public class TapeUtils {


    public static final String NAME_SEPARATOR = "#";
    public static final String DELETED = "DELETED";
    public static final String GZ = ".gz";

    public static URI toURI(TarEntry entry) {
        return toURI(entry.getName());
    }

    public static URI toURI(String filename) {


        int endIndex = filename.indexOf(NAME_SEPARATOR);
        if (endIndex < 0) {
            endIndex = filename.length();
        }
        return URI.create(filename.substring(0, endIndex));
    }


    public static String toFilename(URI id) {
        return id.toString() + NAME_SEPARATOR + System.currentTimeMillis();
    }

    public static String toFilenameGZ(URI id) {
        return toFilename(id)+ GZ;
    }


    public static String toDeleteFilename(URI id) {
        return id.toString() + NAME_SEPARATOR + System.currentTimeMillis() + NAME_SEPARATOR + DELETED;
    }

    public static long getTimestamp(TarEntry entry) {
        String name = entry.getName();
        String[] splits = name.split("["+NAME_SEPARATOR+"\\.]");
        return Long.parseLong(splits[1]);
    }

    public static File toNewName(File file) {
        return new File(file.getParentFile(), "new_" + file.getName());
    }

    public static void compress(File fileToTape, OutputStream destination) throws IOException {
        GzipCompressorOutputStream uncompressor = new GzipCompressorOutputStream(destination);
        final InputStream input = new FileInputStream(fileToTape);
        try {
            IOUtils.copyLarge(input, uncompressor);
        } finally {
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(destination);
        }
    }

    public static long getLengthCompressed(File fileToTape) throws IOException {
        CountingOutputStream counter = new CountingOutputStream(new NullOutputStream());
        GzipCompressorOutputStream uncompressor = new GzipCompressorOutputStream(counter);
        final InputStream input = new FileInputStream(fileToTape);
        try {
            IOUtils.copyLarge(input, uncompressor);
        }
        finally {
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(counter);
        }
        return counter.getBytesWritten();
    }

    public static long getLengthDirect(InputStream stream) throws IOException {
        CountingOutputStream counter = new CountingOutputStream(new NullOutputStream());
        try {
            IOUtils.copyLarge(stream, counter);
        } finally {
            IOUtils.closeQuietly(stream);
            IOUtils.closeQuietly(counter);
        }
        return counter.getBytesWritten();
    }
}
