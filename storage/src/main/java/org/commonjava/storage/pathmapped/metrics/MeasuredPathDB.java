package org.commonjava.storage.pathmapped.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
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
        return decorated.getFileLength( fileSystem, path );
    }

    @Override
    public long getFileLastModified( String fileSystem, String path )
    {
        return decorated.getFileLastModified( fileSystem, path );
    }

    @Override
    public boolean exists( String fileSystem, String path )
    {
        return measure( () -> decorated.exists( fileSystem, path ), "exists" );
    }

    @Override
    public void insert( String fileSystem, String path, Date date, String fileId, long size, String fileStorage,
                        String checksum )
    {
        measure( () -> decorated.insert( fileSystem, path, date, fileId, size, fileStorage, checksum ), "insert" );
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
    public String getStorageFile( String fileSystem, String path )
    {
        return decorated.getStorageFile( fileSystem, path );
    }

    @Override
    public boolean copy( String fromFileSystem, String fromPath, String toFileSystem, String toPath )
    {
        return decorated.copy( fromFileSystem, fromPath, toFileSystem, toPath );
    }

    @Override
    public void makeDirs( String fileSystem, String path )
    {
        measure( () -> decorated.makeDirs( fileSystem, path ), "makeDirs" );
    }

    @Override
    public List<Reclaim> listOrphanedFiles( int limit )
    {
        return decorated.listOrphanedFiles( limit );
    }

    @Override
    public void removeFromReclaim( Reclaim reclaim )
    {
        decorated.removeFromReclaim( reclaim );
    }

    private static final String TIMER = "timer";

    private void measure( Runnable runnable, String metricName )
    {
        if ( metricRegistry != null )
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
            if ( metricRegistry != null )
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

}
