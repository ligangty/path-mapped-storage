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

import org.commonjava.o11yphant.metrics.MetricsManager;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.spi.PathDB;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.commonjava.o11yphant.metrics.util.NameUtils.name;

public class MeasuredPathDB
                implements PathDB
{
    private final PathDB decorated;

    private final MetricsManager metricsManager;

    private final String metricPrefix;

    public MeasuredPathDB( PathDB decorated, MetricsManager metricsManager, String metricPrefix )
    {
        this.decorated = decorated;
        this.metricsManager = metricsManager;
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
    public PathMap getPathMap(String fileSystem, String path) {
        return measure( () -> decorated.getPathMap( fileSystem, path ), "getPathMap" );
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
    public FileType exists( String fileSystem, String path )
    {
        return measure( () -> decorated.exists( fileSystem, path ), "exists" );
    }

    @Override
    public boolean existsFile( String fileSystem, String path )
    {
        return measure( () -> decorated.existsFile( fileSystem, path ), "existsFile" );
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
    public void traverse( String fileSystem, String path, Consumer<PathMap> consumer, int limit, FileType fileType )
    {
        measure( () -> decorated.traverse( fileSystem, path, consumer, limit, fileType ), "traverse" );
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
    public boolean copy(String fromFileSystem, String fromPath, String toFileSystem, String toPath, Date creation, Date expiration) {
        return measure( () -> decorated.copy( fromFileSystem, fromPath, toFileSystem, toPath, creation, expiration ), "copy" );
    }

    @Override
    public void expire(String fileSystem, String path, Date expiration)
    {
        measure( () -> decorated.expire( fileSystem, path, expiration ), "expire" );
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

    private void measure( Runnable runnable, String metricName )
    {
        if ( metricsManager != null && isMetricEnabled( metricName ) )
        {
            metricsManager.wrapWithStandardMetrics( () -> {
                runnable.run();
                return null;
            }, () -> name( metricPrefix, metricName ) );
        }
        else
        {
            runnable.run();
        }
    }

    private <T> T measure( Supplier<T> methodSupplier, String metricName )
    {
        if ( metricsManager != null && isMetricEnabled( metricName ) )
        {
            return metricsManager.wrapWithStandardMetrics( methodSupplier, () -> name( metricPrefix, metricName ) );
        }
        else
        {
            return methodSupplier.get();
        }
    }

    protected boolean isMetricEnabled( String metricName )
    {
        return true;
    }

}
