/**
 * Copyright (C) 2019 Red Hat, Inc. (nos-devel@redhat.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonjava.storage.pathmapped.core;

import org.apache.commons.lang.StringUtils;
import org.commonjava.cdi.util.weft.NamedThreadFactory;
import org.commonjava.storage.pathmapped.config.PathMappedStorageConfig;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.spi.FileInfo;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.commonjava.storage.pathmapped.spi.PhysicalStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.ROOT_DIR;

public class PathMappedFileManager implements Closeable
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final PathDB pathDB;

    private final PhysicalStore physicalStore;

    private final PathMappedStorageConfig config;

    private ScheduledExecutorService gcThreadPool;

    private String deduplicatePattern;

    public PathMappedFileManager( PathMappedStorageConfig config, PathDB pathDB, PhysicalStore physicalStore )
    {
        this.pathDB = pathDB;
        this.physicalStore = physicalStore;
        this.config = config;

        int gcIntervalInMinutes = config.getGCIntervalInMinutes();
        if ( gcIntervalInMinutes > 0 )
        {
            logger.info( "Start path-mapped GC thread, gcIntervalInMinutes: {}", gcIntervalInMinutes );
            int priority = 4;
            String name = "path-mapped-gc";
            gcThreadPool = Executors.newScheduledThreadPool( 1, new NamedThreadFactory( name, new ThreadGroup( name ),
                                                                                        true, priority ) );
            int initialDelay = gcIntervalInMinutes;
            gcThreadPool.scheduleAtFixedRate( () -> {
                gc();
            }, initialDelay, gcIntervalInMinutes, TimeUnit.MINUTES );
        }

        deduplicatePattern = config.getDeduplicatePattern();
    }

    public InputStream openInputStream( String fileSystem, String path ) throws IOException
    {
        String storageFile = pathDB.getStorageFile( fileSystem, path );
        if ( storageFile == null )
        {
            throw new IOException( String.format(
                    "Could not open input stream to for path %s - %s: path-mapped physical file does not exist.", fileSystem, path) );
        }
        final InputStream stream = physicalStore.getInputStream( storageFile );
        if ( stream == null )
        {
            throw new IOException( String.format
                    ("Could not open input stream to for path %s - %s: path-mapped physical file does not exist or the path is a directory.", fileSystem, path) );
        }
        return stream;
    }

    public OutputStream openOutputStream( String fileSystem, String path ) throws IOException
    {
        return openOutputStream( fileSystem, path, 0, TimeUnit.SECONDS ); // 0 is never timeout
    }

    public OutputStream openOutputStream( String fileSystem, String path, long timeout, TimeUnit timeoutUnit )
                    throws IOException
    {
        FileInfo fileInfo = physicalStore.getFileInfo( fileSystem, path );
        String checksumAlgorithm = null;
        if ( deduplicatePattern != null && fileSystem.matches( deduplicatePattern ) )
        {
            checksumAlgorithm = config.getFileChecksumAlgorithm();
        }
        try
        {
            return new PathDBOutputStream( pathDB, physicalStore, fileSystem, path, fileInfo,
                                           physicalStore.getOutputStream( fileInfo ),
                                           checksumAlgorithm,
                                           timeoutUnit.toMillis( timeout ));
        }
        catch ( NoSuchAlgorithmException e )
        {
            throw new IOException( "Error: checksum checking not correct", e );
        }
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

    public String[] list( String fileSystem, String path )
    {
        return list( fileSystem, path, PathDB.FileType.all );
    }

    public String[] list( String fileSystem, String path, boolean recursive, int limit )
    {
        return list( fileSystem, path, recursive, limit, PathDB.FileType.all );
    }

    public String[] list( String fileSystem, String path, PathDB.FileType fileType )
    {
        return list( fileSystem, path, false, 0, fileType );
    }

    public String[] list( String fileSystem, String path, boolean recursive, int limit, PathDB.FileType fileType )
    {
        if ( path == null )
        {
            return new String[] {};
        }
        List<PathMap> paths;
        if ( recursive )
        {
            paths = pathDB.list( fileSystem, path, true, limit, fileType );
            return paths.stream().map( x -> {
                String cutParentPath = cutParentPath( path, x.getParentPath() );
                if ( isBlank( cutParentPath ) )
                {
                    return x.getFilename();
                }
                return cutParentPath + "/" + x.getFilename();
            } ).toArray( String[]::new );
        }
        else
        {
            paths = pathDB.list( fileSystem, path, fileType );
            return paths.stream().map( x -> x.getFilename() ).toArray( String[]::new );
        }
    }

    /*
     * Format listing result. e.g, if path=/foo, parentPath=/foo/bar/1, result will be bar/1
     * The parentPath always has heading / so we make sure root has that too. /foo/1.0
     */
    private String cutParentPath( String rootPath, String parentPath )
    {
        if ( !rootPath.startsWith( "/" ) )
        {
            rootPath = "/" + rootPath;
        }
        String ret = parentPath.replaceFirst( rootPath, "" );
        if ( ret.startsWith( "/" ) )
        {
            ret = ret.replaceFirst( "/", "" );
        }
        return ret;
    }

    public long getFileLength( String fileSystem, String path )
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
        // NOS-2289 After checking the database, I found that there are some mismatch between database entries
        //          and physical store files. There are entries in database, but the corresponding physical files
        //          missed in physical location. So I think we need to check real existence of from physical level
        //          but not just from db level to make sure if it really exists.
        if ( StringUtils.isBlank( path ) )
        {
            logger.warn( "Checking existence of an empty path for file system {}, which is considered as not existed", fileSystem );
            return false;
        }
        if ( isDirectory( fileSystem, path ) )
        {
            return pathDB.exists( fileSystem, path );
        }
        final String storageFile = pathDB.getStorageFile( fileSystem, path );
        return physicalStore.exists( storageFile );
    }

    public boolean isDirectory( String fileSystem, String path )
    {
        if ( path == null )
        {
            return false;
        }
        if ( ROOT_DIR.equals( path ) )
        {
            return true;
        }
        return pathDB.isDirectory( fileSystem, path );
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
        while ( true )
        {
            int batchSize = config.getGCBatchSize();
            List<Reclaim> reclaims = pathDB.listOrphanedFiles( batchSize );
            int size = reclaims.size();
            logger.debug( "Get reclaims for GC, size: {}", size );
            if ( size <= 0 )
            {
                break;
            }
            else if ( batchSize > 0 && size < batchSize )
            {
                logger.debug( "Get reclaims but less than batch size {}. Break.", batchSize );
                break;
            }
            reclaims.forEach( ( reclaim ) -> {
                FileInfo fileInfo = new FileInfo();
                fileInfo.setFileId( reclaim.getFileId() );
                fileInfo.setFileStorage( reclaim.getStorage() );
                boolean result = physicalStore.delete( fileInfo );
                if ( result )
                {
                    logger.debug( "Delete from physicalStore, fileInfo: {}", fileInfo );
                    pathDB.removeFromReclaim( reclaim );
                }
                gcResults.put( fileInfo, result );
            } );
        }
        return gcResults;
    }

    @Override
    public void close() throws IOException
    {
        if ( pathDB instanceof Closeable )
        {
            ( (Closeable) pathDB ).close();
        }
    }
}
