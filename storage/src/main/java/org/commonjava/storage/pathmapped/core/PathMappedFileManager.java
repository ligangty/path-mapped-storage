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

import org.commonjava.storage.pathmapped.config.PathMappedStorageConfig;
import org.commonjava.storage.pathmapped.model.Filesystem;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.spi.FileInfo;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.commonjava.storage.pathmapped.spi.PathDBAdmin;
import org.commonjava.storage.pathmapped.spi.PhysicalStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private final String commonFileExtensions;

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
            gcThreadPool = Executors.newScheduledThreadPool( 1 );
            int initialDelay = gcIntervalInMinutes;
            gcThreadPool.scheduleAtFixedRate( () -> {
                gc();
            }, initialDelay, gcIntervalInMinutes, TimeUnit.MINUTES );
        }

        deduplicatePattern = config.getDeduplicatePattern();

        commonFileExtensions = config.getCommonFileExtensions();
    }

    public Set<String> getFileSystemContainingDirectory( Collection<String> candidates, String path )
    {
        if ( !path.endsWith( "/" ) )
        {
            path += "/";
        }
        return pathDB.getFileSystemContaining( candidates, path );
    }

    public Set<String> getFileSystemContaining( Collection<String> candidates, String path )
    {
        return pathDB.getFileSystemContaining( candidates, path );
    }

    public String getFirstFileSystemContaining( List<String> candidates, String path )
    {
        return pathDB.getFirstFileSystemContaining( candidates, path );
    }

    public InputStream openInputStream( String fileSystem, String path ) throws IOException
    {
        String storageFile = pathDB.getStorageFile( fileSystem, path );
        if ( storageFile == null )
        {
            throw new IOException( String.format(
                    "Could not open input stream to for path %s - %s: path-mapped file does not exist.", fileSystem, path) );
        }
        final InputStream stream = physicalStore.getInputStream( storageFile );
        if ( stream == null )
        {
            throw new IOException( String.format
                    ("Could not open input stream to for path %s - %s: path-mapped physical file does not exist.", fileSystem, path) );
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
            return new BufferedOutputStream( new PathDBOutputStream( pathDB, physicalStore,
                                           fileSystem, path, fileInfo,
                                           physicalStore.getOutputStream( fileInfo ),
                                           checksumAlgorithm,
                                           timeoutUnit.toMillis( timeout )) );
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
        if ( isBlank( path ) )
        {
            return false;
        }

        boolean exists = false;

        if ( commonFileExtensions != null && path.matches( commonFileExtensions ) )
        {
            // query file
            exists = pathDB.existsFile( fileSystem, path );
        }
        else
        {
            // query both file and dir
            PathDB.FileType type = pathDB.exists( fileSystem, path );
            if ( type != null )
            {
                exists = true;
            }
            if ( type == PathDB.FileType.dir )
            {
                return true;
            }
        }

        if ( exists )
        {
            // check expiration
            String storageFile = pathDB.getStorageFile( fileSystem, path );
            if ( storageFile != null )
            {
                if ( !config.isPhysicalFileExistenceCheckEnabled() )
                {
                    return true;
                }
                // we used to check the physical file during exist check. Here we ignore it because pathDB should be the
                // only source-of-truth. If pathDB entry exists but physical file missing, that is a bug and IOException is thrown.
                // we will run this code to see if any problem. ruhan Apr 20, 2020

                // Add the physical file check back. We hit data corruption on some environment
                // and this check will help Indy recover from bad state. We agree to make it configurable, e.g,
                // we don't need it on prod, just enable it on devel where the data corruption occurred.
                // ruhan May 6, 2022
                if ( physicalStore.exists( storageFile ) )
                {
                    return true;
                }
                else
                {
                    logger.error( "File in pathDB but physical file missing! fileSystem: {}, path: {}, storageFile: {}",
                                 fileSystem, path, storageFile );
                    return false;
                }
            }
        }
        return false;
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

    public PathDB getPathDB()
    {
        return pathDB;
    }

    public Filesystem getFilesystem(String filesystem)
    {
        if (pathDB instanceof PathDBAdmin ) {
            return ((PathDBAdmin)pathDB).getFilesystem(filesystem);
        }
        return null;
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
