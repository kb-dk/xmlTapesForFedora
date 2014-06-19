package dk.statsbiblioteket.metadatarepository.xmltapes.common;

import org.kamranzafar.jtar.TarEntry;

import java.io.File;
import java.io.IOException;
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
    protected static final String NEW_ = "new_";

    /**
     * Get the canonical ID for a tar entry
     * @param entry the entry
     * @return the canonical id
     */
    public static URI getIdFromTarEntry(TarEntry entry) {
        String filename = entry.getName();
        if (!filename.contains("/")) {
            filename = decode(filename);
        }
        int endIndex = filename.indexOf(NAME_SEPARATOR);
        if (endIndex < 0) {
            endIndex = filename.indexOf(GZ);
            if (endIndex < 0){
                endIndex = filename.length();
            }
        }
        return uri(filename.substring(0, endIndex));

    }

    private static URI uri(String name)  {
        try {
            return new URI(name);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to parse '"+name+"' as uri",e);
        }
    }

    /**
     * Extract the canonical Id from a file in the cache
     * @param cacheFile the file in cache
     * @return the canonical ID
     */
    public static URI getIdfromFile(File cacheFile) {
        String name = cacheFile.getName();
        name = decode(name);
        name = name.replaceAll(Pattern.quote(GZ) + "$", "");
        name = name.replaceAll(Pattern.quote(NAME_SEPARATOR + DELETED) + "$", "");
        return uri(name);
    }


    /**
     * Get the semi canonical id of a file in the cache. The returned id will be the canonical id with an optional
     * postfix of #DELETED
     * @param cacheFile the file in cache
     * @return the semi canonical id
     */
    protected static URI getIDfromFileWithDeleted(File cacheFile) {
        String name = cacheFile.getName();
        name = decode(name);
        name = name.replaceAll(Pattern.quote(GZ) + "$", "");
        return uri(name);
    }


    /**
     * Convert the canonical Id to a encoded timestamped value, suitable for the tapes
     * @param id the id
     * @return the converted id
     */
    public static String getTimestampedFilenameFromId(URI id) {
        return encode(id) + NAME_SEPARATOR + System.currentTimeMillis() + GZ;
    }

    public static String getDeleteTimestampedFilenameFromId(URI id) {
        return encode(id) + NAME_SEPARATOR + System.currentTimeMillis() + NAME_SEPARATOR + DELETED + GZ;
    }



    /**
         * Convert the canonical id to a filename, suitable for the stages where the record is stored as a file
         * @param id the canonical ID
         * @return the id as a filename
         */
    public static String getFilenameFromId(URI id) {
        return encode(id) +  GZ;
    }

    /**
     * Convert the canonical id to a filename, suitable for the stages where the record is stored as a file
     *
     * @param id the canonical ID
     *
     * @return the id as a filename
     */
    public static String getDeleteFilenameFromId(URI id) {
        return encode(id) + NAME_SEPARATOR + DELETED + GZ;
    }


    /**
     * Encode the id to prevent special characters causing problems
     * @param id the canonical ID
     * @return the ID url encoded
     */
    public static String encode(URI id) {
        try {
            return URLEncoder.encode(id.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Error("UTF-8 not known",e);
        }
    }

    private static String decode(String name) {
        try {
            return URLDecoder.decode(name, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Error("UTF-8 not known", e);
        }
    }


    /**
     * Generate a file name with the new prefix from another filename
     * @param file the file to "new"
     * @return the new_filename
     */
    public static File toNewName(File file) {
        return new File(file.getParentFile(), NEW_ + file.getName());
    }


    public static File makeTempFile(URI id, File temp_dir) throws IOException {
        temp_dir.mkdirs();
        File tempfile = File.createTempFile(encode(id),
                GZ, temp_dir);
        tempfile.deleteOnExit();
        return tempfile;
    }

    public static boolean isZipped(TarEntry tarEntry) {
        return tarEntry.getName().endsWith(GZ);
    }

    public static boolean isZipped(File fileToTape) {
        return fileToTape.getName().endsWith(GZ);
    }

    public static boolean isDeleteFile(File tapingFile) {
        return isDelete(tapingFile.getName());
    }

    public static boolean isDeleteEntry(TarEntry entry) {
        return isDelete(entry.getName());
    }

    static URI stripDeleted(URI uri) {
        return URI.create(uri.toString().replaceAll(Pattern.quote(NAME_SEPARATOR + DELETED), ""));
    }

    private static boolean isDelete(String name) {
        return name.endsWith(NAME_SEPARATOR + DELETED) || name.endsWith(NAME_SEPARATOR + DELETED + GZ);
    }

    static boolean isDelete(URI uri) {
        return isDelete(uri.toString());
    }

    public static File getStoredFile(File storeDir, URI id) throws IOException {
        final String filename = getFilenameFromId(id);
        final File file = new File(storeDir, filename);
        final File fileNew = toNewName(file);
        if (!file.exists() && fileNew.exists()) {
            return fileNew;
        }
        return file;
    }

    public static File getStoredFileDeleted(File storeDir, URI id) {
        return new File(storeDir,getDeleteFilenameFromId(id));
    }
}
