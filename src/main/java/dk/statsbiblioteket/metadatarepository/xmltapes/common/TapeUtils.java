package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.regex.Pattern;

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
        if (filename.endsWith(GZ)){
            filename = filename.replace(GZ,"");
        }
        return URI.create(filename.substring(0, endIndex));
    }


    public static String toTimestampedFilename(URI id) {
        return encode(id) + NAME_SEPARATOR + System.currentTimeMillis() + GZ;
    }

    public static String toFilename(URI id) {
        return encode(id) +  GZ;
    }

    private static String encode(URI id) {
        try {
            return URLEncoder.encode(id.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Error("UTF-8 not known",e);
        }
    }


    public static String toDeleteFilename(URI id) {
        return encode(id) + NAME_SEPARATOR + System.currentTimeMillis() + NAME_SEPARATOR + DELETED + GZ;
    }

    public static long getTimestamp(TarEntry entry) {
        String name = entry.getName();
        String[] splits = name.split("["+NAME_SEPARATOR+"\\.]");
        return Long.parseLong(splits[1]);
    }

    public static File toNewName(File file) {
        return new File(file.getParentFile(), "new_" + file.getName());
    }

    public static void copy(File fileToTape, OutputStream destination) throws IOException {
        final InputStream input = new FileInputStream(fileToTape);
        try {
            IOUtils.copyLarge(input, destination);
        } finally {
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(destination);
        }
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

    public static long getLengthUncompressed(File fileToTape) throws IOException {
        CountingOutputStream counter = new CountingOutputStream(new NullOutputStream());
        final InputStream input = new GzipCompressorInputStream(new FileInputStream(fileToTape));
        try {
            IOUtils.copyLarge(input, counter);
        } finally {
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

    public static URI getIDfromFile(File cacheFile) {

        try {
            String name = cacheFile.getName();
            name = URLDecoder.decode(name, "UTF-8");
            name = name.replaceAll(Pattern.quote(GZ)+"$","");
            name = name.replaceAll(Pattern.quote("#" + DELETED)+"$","");
            return new URI(name);
        } catch (URISyntaxException e) {
            return null;
        } catch (UnsupportedEncodingException e) {
            throw new Error(e);
        }
    }

    protected static URI getIDfromFileWithDeleted(File cacheFile) {

          try {
              String name = cacheFile.getName();
              name = URLDecoder.decode(name, "UTF-8");
              name = name.replaceAll(Pattern.quote(GZ) + "$", "");
              return new URI(name);
          } catch (URISyntaxException e) {
              return null;
          } catch (UnsupportedEncodingException e) {
              throw new Error(e);
          }
      }
}
