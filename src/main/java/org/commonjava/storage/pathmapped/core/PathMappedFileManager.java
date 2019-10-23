package org.commonjava.storage.pathmapped.core;

import org.commonjava.storage.pathmapped.config.PathMappedStorageConfig;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.commonjava.storage.pathmapped.spi.PhysicalStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PathMappedFileManager
{
    private final PathDB pathDB;

    private final PhysicalStore physicalStore;

    private final PathMappedStorageConfig config;

    public PathMappedFileManager( PathMappedStorageConfig config, PathDB pathDB, PhysicalStore physicalStore )
    {
        this.pathDB = pathDB;
        this.physicalStore = physicalStore;
        this.config = config;
    }

    public InputStream openInputStream( String fileSystem, String path ) throws IOException
    {
        String storageFile = pathDB.getStorageFile( fileSystem, path );
        if ( storageFile == null )
        {
            return null;
        }
        return physicalStore.getInputStream( storageFile );
    }

    public OutputStream openOutputStream( String fileSystem, String path ) throws IOException
    {
        FileInfo fileInfo = physicalStore.getFileInfo( fileSystem, path );
        return new PathDBOutputStream( pathDB, physicalStore, fileSystem, path, fileInfo,
                                       physicalStore.getOutputStream( fileInfo ) );
    }

    public boolean delete( String fileSystem, String path )
    {
        return pathDB.delete( fileSystem, path );
    }

    public void cleanupCurrentThread()
    {
    }

    public void startReporting()
    {
    }

    public void stopReporting()
    {
    }

    // append '/' to directory names if not has
    public String[] list( String fileSystem, String path )
    {
        if ( path == null )
        {
            return new String[] {};
        }
        List<PathMap> paths = pathDB.list( fileSystem, path );
        return paths.stream().map( x -> {
            String p = x.getFilename();
            if ( x.getFileId() == null && !p.endsWith( "/" ) )
            {
                p += "/";
            }
            return p;
        } ).toArray( String[]::new );
    }

    public int getFileLength( String fileSystem, String path )
    {
        if ( path == null )
        {
            return 0;
        }
        return pathDB.getFileLength( fileSystem, path );
    }

    public long getFileLastModified( String fileSystem, String path )
    {
        if ( path == null )
        {
            return -1L;
        }
        return pathDB.getFileLastModified( fileSystem, path );
    }

    public boolean exists( String fileSystem, String path )
    {
        if ( path == null )
        {
            return false;
        }
        return pathDB.exists( fileSystem, path );
    }

    public boolean isDirectory( String fileSystem, String path )
    {
        if ( path == null )
        {
            return false;
        }
        final String pathWithSlash = path.endsWith( "/" ) ? path : path + "/";
        return pathDB.isDirectory( fileSystem, pathWithSlash );
    }

    public boolean isFile( String fileSystem, String path )
    {
        if ( path == null )
        {
            return false;
        }
        return pathDB.isFile( fileSystem, path );
    }

    public void copy( String fromFileSystem, String fromPath, String toFileSystem, String toPath )
    {
        pathDB.copy( fromFileSystem, fromPath, toFileSystem, toPath );
    }

    public void makeDirs( String fileSystem, String path )
    {
        pathDB.makeDirs( fileSystem, path );
    }

    public String getFileStoragePath( String fileSystem, String path )
    {
        return pathDB.getStorageFile( fileSystem, path );
    }

    public Map<FileInfo, Boolean> gc()
    {
        Map<FileInfo, Boolean> gcResults = new HashMap<>();
        List<Reclaim> reclaims = pathDB.listOrphanedFiles();
        reclaims.forEach( ( reclaim ) -> {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFileId( reclaim.getFileId() );
            fileInfo.setFileStorage( reclaim.getStorage() );
            boolean result = physicalStore.delete( fileInfo );
            if ( result )
            {
                pathDB.removeFromReclaim( reclaim );
            }
            gcResults.put( fileInfo, result );
        } );
        return gcResults;
    }

}
