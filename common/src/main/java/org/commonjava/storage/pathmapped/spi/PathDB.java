package org.commonjava.storage.pathmapped.spi;

import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;

import java.util.Date;
import java.util.List;

public interface PathDB
{
    enum FileType {
        all, file, dir;
    };

    List<PathMap> list( String fileSystem, String path, FileType fileType );

    List<PathMap> list( String fileSystem, String path, boolean recursive, int limit, FileType fileType );

    long getFileLength( String fileSystem, String path );

    long getFileLastModified( String fileSystem, String path );

    boolean exists( String fileSystem, String path );

    void insert( String fileSystem, String path, Date date, String fileId, long size, String fileStorage, String checksum );

    boolean isDirectory( String fileSystem, String path );

    boolean isFile( String fileSystem, String path );

    boolean delete( String fileSystem, String path );

    String getStorageFile( String fileSystem, String path );

    boolean copy( String fromFileSystem, String fromPath, String toFileSystem, String toPath );

    void makeDirs( String fileSystem, String path );

    List<Reclaim> listOrphanedFiles( int limit );

    default List<Reclaim> listOrphanedFiles() { return listOrphanedFiles( 0 ); }

    void removeFromReclaim( Reclaim reclaim );

}
