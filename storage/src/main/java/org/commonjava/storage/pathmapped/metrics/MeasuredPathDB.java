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
package org.commonjava.storage.pathmapped.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.codahale.metrics.MetricRegistry.name;

public class MeasuredPathDB
                implements PathDB
{
    private final PathDB decorated;

    private final MetricRegistry metricRegistry;

    private final String metricPrefix;

    public MeasuredPathDB( PathDB decorated, MetricRegistry metricRegistry, String metricPrefix )
    {
        this.decorated = decorated;
        this.metricRegistry = metricRegistry;
        this.metricPrefix = metricPrefix;
    }

    @Override
    public List<PathMap> list( String fileSystem, String path, FileType fileType )
    {
        return measure( () -> decorated.list( fileSystem, path, fileType ), "list" );
    }

    @Override
    public List<PathMap> list( String fileSystem, String path, boolean recursive, int limit, FileType fileType )
    {
        return measure( () -> decorated.list( fileSystem, path, recursive, limit, fileType ), "listRecursively" );
    }

    @Override
    public long getFileLength( String fileSystem, String path )
    {
        return measure( () -> decorated.getFileLength( fileSystem, path ), "getFileLength" );
    }

    @Override
    public long getFileLastModified( String fileSystem, String path )
    {
        return measure( () -> decorated.getFileLastModified( fileSystem, path ), "getFileLastModified" );
    }

    @Override
    public boolean exists( String fileSystem, String path )
    {
        return measure( () -> decorated.exists( fileSystem, path ), "exists" );
    }

    @Override
    public void insert( String fileSystem, String path, Date creation, Date expiration, String fileId, long size, String fileStorage,
                        String checksum )
    {
        measure( () -> decorated.insert( fileSystem, path, creation, expiration, fileId, size, fileStorage, checksum ), "insert" );
    }

    @Override
    public boolean isDirectory( String fileSystem, String path )
    {
        return measure( () -> decorated.isDirectory( fileSystem, path ), "isDirectory" );
    }

    @Override
    public boolean isFile( String fileSystem, String path )
    {
        return measure( () -> decorated.isFile( fileSystem, path ), "isFile" );
    }

    @Override
    public boolean delete( String fileSystem, String path )
    {
        return measure( () -> decorated.delete( fileSystem, path ), "delete" );
    }

    @Override
    public Set<String> getFileSystemContaining( Collection<String> candidates, String path )
    {
        return measure( () -> decorated.getFileSystemContaining( candidates, path ), "getFileSystemContaining" );
    }

    @Override
    public String getFirstFileSystemContaining( List<String> candidates, String path )
    {
        return measure( () -> decorated.getFirstFileSystemContaining( candidates, path ), "getFirstFileSystemContaining" );
    }

    @Override
    public Set<String> getFileSystemContainingDirectory( Collection<String> candidates, String path )
    {
        return measure( () -> decorated.getFileSystemContainingDirectory( candidates, path ), "getFileSystemContainingDirectory" );
    }

    @Override
    public String getStorageFile( String fileSystem, String path )
    {
        return measure( () -> decorated.getStorageFile( fileSystem, path ), "getStorageFile" );
    }

    @Override
    public boolean copy( String fromFileSystem, String fromPath, String toFileSystem, String toPath )
    {
        return measure( () -> decorated.copy( fromFileSystem, fromPath, toFileSystem, toPath ), "copy" );
    }

    @Override
    public void makeDirs( String fileSystem, String path )
    {
        measure( () -> decorated.makeDirs( fileSystem, path ), "makeDirs" );
    }

    @Override
    public List<Reclaim> listOrphanedFiles( int limit )
    {
        return measure( () -> decorated.listOrphanedFiles( limit ), "listOrphanedFiles" );
    }

    @Override
    public void removeFromReclaim( Reclaim reclaim )
    {
        decorated.removeFromReclaim( reclaim );
    }

    private static final String TIMER = "timer";

    private void measure( Runnable runnable, String metricName )
    {
        if ( metricRegistry != null && isMetricEnabled( metricName ) )
        {
            Timer.Context context = metricRegistry.timer( name( metricPrefix, metricName, TIMER ) ).time();
            try
            {
                runnable.run();
            }
            finally
            {
                context.stop();
            }
        }
        else
        {
            runnable.run();
        }
    }

    private <T> T measure( Callable<T> callable, String metricName )
    {
        try
        {
            if ( metricRegistry != null && isMetricEnabled( metricName ) )
            {
                Timer.Context context = metricRegistry.timer( name( metricPrefix, metricName, TIMER ) ).time();
                try
                {
                    return callable.call();
                }
                finally
                {
                    context.stop();
                }
            }
            else
            {
                return callable.call();
            }
        }
        catch ( Exception e )
        {
            Logger logger = LoggerFactory.getLogger( getClass() );
            logger.warn( "Call failed", e );
            return null;
        }
    }

    protected boolean isMetricEnabled( String metricName )
    {
        return true;
    }

}
