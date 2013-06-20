/**
 * Documentation about the design
 * Fedora is expected to not write a new version of an object before it has finished writing the previous version
 * This invariant seems to hold.
 *
 * Every write is to a (new) temp file. This file is named somehow, the name does not matter.
 * The Buffer holds a mapping from the temp name to the object id
 *
 * When the outputstream to the temp file is closed, the file is atomically moved to a name corresponding to the object
 * id. This uses the filesystem move command, and must therefore not cross partitions.
 *
 * Reading a file will consist of a lookup of the above mentioned file. If this does not exist, the taper thread cache
 * is queried instead. If the file is still not found the tape system is queried instead.
 * As far as we know, the move command will not interfere with reading the file, as the FileInputstream will continue
 * on the old inode.
 *
 * A timer thread will periodically move files to the tapes. For each finished file, it will move it to it's own cache
 * area. Then it will move this file to the tape, and delete it from it's cache area.
 * The cache area is nessesary to ensure that the subsequent delete will not delete a file just written.

 */