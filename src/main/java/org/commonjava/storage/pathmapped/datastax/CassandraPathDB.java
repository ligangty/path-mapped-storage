package org.commonjava.storage.pathmapped.datastax;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import org.commonjava.storage.pathmapped.config.PathMappedStorageConfig;
import org.commonjava.storage.pathmapped.datastax.model.DtxFileChecksum;
import org.commonjava.storage.pathmapped.datastax.model.DtxPathMap;
import org.commonjava.storage.pathmapped.datastax.model.DtxReclaim;
import org.commonjava.storage.pathmapped.datastax.model.DtxReverseMap;
import org.commonjava.storage.pathmapped.model.FileChecksum;
import org.commonjava.storage.pathmapped.model.PathMap;
import org.commonjava.storage.pathmapped.model.Reclaim;
import org.commonjava.storage.pathmapped.model.ReverseMap;
import org.commonjava.storage.pathmapped.spi.PathDB;
import org.commonjava.storage.pathmapped.util.PathMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.commonjava.storage.pathmapped.util.CassandraPathDBUtils.*;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.ROOT_DIR;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.getFilename;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.getParentPath;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.getParentsBottomUp;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.marshall;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.normalize;
import static org.commonjava.storage.pathmapped.util.PathMapUtils.normalizeParentPath;

public class CassandraPathDB
                implements PathDB, Closeable
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private Cluster cluster;

    private Session session;

    private Mapper<DtxPathMap> pathMapMapper;

    private Mapper<DtxReverseMap> reverseMapMapper;

    private Mapper<DtxReclaim> reclaimMapper;

    private Mapper<DtxFileChecksum> fileChecksumMapper;

    private final PathMappedStorageConfig config;

    private final String keyspace;

    private final PreparedStatement preparedSingleExistQuery, preparedDoubleExistQuery, preparedListQuery;

    public CassandraPathDB( PathMappedStorageConfig config )
    {
        this.config = config;

        String host = (String) config.getProperty( PROP_CASSANDRA_HOST );
        int port = (Integer) config.getProperty( PROP_CASSANDRA_PORT );
        String username = (String) config.getProperty( PROP_CASSANDRA_USER );
        String password = (String) config.getProperty( PROP_CASSANDRA_PASS );

        Cluster.Builder builder = Cluster.builder().withoutJMXReporting().addContactPoint( host ).withPort( port );
        if ( isNotBlank( username ) && isNotBlank( password ) )
        {
            logger.debug( "Build with credentials, user: {}, pass: ****", username );
            builder.withCredentials( username, password );
        }
        cluster = builder.build();

        logger.debug( "Connecting to Cassandra, host:{}, port:{}", host, port );
        session = cluster.connect();

        keyspace = (String) config.getProperty( PROP_CASSANDRA_KEYSPACE );

        session.execute( getSchemaCreateKeyspace( keyspace ) );
        session.execute( getSchemaCreateTablePathmap( keyspace ) );
        session.execute( getSchemaCreateTableReversemap( keyspace ) );
        session.execute( getSchemaCreateTableReclaim( keyspace ) );
        session.execute( getSchemaCreateTableFileChecksum( keyspace ) );

        MappingManager manager = new MappingManager( session );

        pathMapMapper = manager.mapper( DtxPathMap.class, keyspace );
        reverseMapMapper = manager.mapper( DtxReverseMap.class, keyspace );
        reclaimMapper = manager.mapper( DtxReclaim.class, keyspace );
        fileChecksumMapper = manager.mapper( DtxFileChecksum.class, keyspace );

        preparedSingleExistQuery = session.prepare(
                "SELECT count(*) FROM " + keyspace + ".pathmap WHERE filesystem=? and parentpath=? and filename=?;" );

        preparedDoubleExistQuery = session.prepare( "SELECT count(*) FROM " + keyspace
                                                            + ".pathmap WHERE filesystem=? and parentpath=? and filename in (?,?);" );

        preparedListQuery =
                session.prepare( "SELECT * FROM " + keyspace + ".pathmap WHERE filesystem=? and parentpath=?;" );

    }

    @Override
    public void close()
    {
        session.close();
        cluster.close();
        logger.debug( "Cassandra connection closed" );
    }

    public Session getSession()
    {
        return session;
    }

    /**
     * List files under specified path.
     */
    public List<PathMap> list( String fileSystem, String path )
    {
        String parentPath = normalizeParentPath( path );
        BoundStatement bound = preparedListQuery.bind( fileSystem, parentPath );
        ResultSet result = session.execute( bound );
        Result<DtxPathMap> ret = pathMapMapper.map( result );
        return new ArrayList<>( ret.all() );
    }

    @Override
    public long getFileLength( String fileSystem, String path )
    {
        PathMap pathMap = getPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return pathMap.getSize();
        }
        return -1;
    }

    private DtxPathMap getPathMap( String fileSystem, String path )
    {
        String parentPath = getParentPath( path );
        String filename = getFilename( path );

        if ( parentPath == null || filename == null )
        {
            logger.debug( "getPathMap::null, parentPath:{}, filename:{}", parentPath, filename );
            return null;
        }
        return pathMapMapper.get( fileSystem, parentPath, filename );
    }

    @Override
    public long getFileLastModified( String fileSystem, String path )
    {
        PathMap pathMap = getPathMap( fileSystem, path );
        if ( pathMap != null && pathMap.getFileId() != null )
        {
            return pathMap.getCreation().getTime();
        }
        return -1;
    }

    /**
     * Check if the specified path exist. If the path does not end with /, e.g., "foo/bar", we need to check both "foo/bar"
     * and "foo/bar/".
     */
    @Override
    public boolean exists( String fileSystem, String path )
    {
        if ( ROOT_DIR.equals( path ) )
        {
            return true;
        }
        String parentPath = getParentPath( path );
        String filename = getFilename( path );

        BoundStatement bound;
        if ( filename.endsWith( "/" ) )
        {
            bound = preparedSingleExistQuery.bind( fileSystem, parentPath, filename );
        }
        else
        {
            bound = preparedDoubleExistQuery.bind( fileSystem, parentPath, filename, filename + "/" );
        }
        ResultSet result = session.execute( bound );
        long count = result.one().get( 0, Long.class );
        return count > 0;
    }

    @Override
    public void insert( String fileSystem, String path, Date date, String fileId, long size, String fileStorage, String checksum )
    {
        DtxPathMap pathMap = new DtxPathMap();
        pathMap.setFileSystem( fileSystem );
        String parentPath = getParentPath( path );
        String filename = getFilename( path );
        pathMap.setParentPath( parentPath );
        pathMap.setFilename( filename );
        pathMap.setCreation( date );
        pathMap.setFileId( fileId );
        pathMap.setFileStorage( fileStorage );
        pathMap.setSize( size );
        pathMap.setChecksum( checksum );
        insert( pathMap, checksum );
    }

    @Override
    public void insert( PathMap pathMap, String checksum )
    {
        logger.debug( "Insert: {}", pathMap );

        String fileSystem = pathMap.getFileSystem();
        String parent = pathMap.getParentPath();

        makeDirs( fileSystem, parent );

        String path = normalize( parent, pathMap.getFilename() );
        PathMap prev = getPathMap( fileSystem, path );
        final boolean existedPathMap = prev != null;
        if ( existedPathMap )
        {
            delete( fileSystem, path );
        }

        final FileChecksum existedChecksum = fileChecksumMapper.get( checksum );

        if ( existedChecksum != null )
        {
            logger.info( "File checksum conflict, should use existed file storage" );
            final String deprecatedStorage = pathMap.getFileStorage();
            ( (DtxPathMap) pathMap ).setFileStorage( existedChecksum.getStorage() );
            ( (DtxPathMap) pathMap ).setFileId( existedChecksum.getFileId() );
            ( (DtxPathMap) pathMap ).setChecksum( existedChecksum.getChecksum() );
            // Need to mark the generated file storage path as reclaimed to remove it.
            final String deprecatedFileId = PathMapUtils.getRandomFileId();
            reclaim( deprecatedFileId, deprecatedStorage );
        }
        else
        {
            logger.debug( "File checksum not exists, marked current file {} as primary", pathMap );
            fileChecksumMapper.save( new DtxFileChecksum( checksum, pathMap.getFileId(), pathMap.getFileStorage() ) );
        }

        pathMapMapper.save( (DtxPathMap) pathMap );

        // insert reverse mapping
        addToReverseMap( pathMap.getFileId(), marshall( fileSystem, path ) );
    }

    /**
     * There is a short cut mainly due to performance consideration. If path ends with /, it is safe to assume it is directory.
     * The general use case is caller to recursively list a dir and for all sub-folders we return a name with slash.
     */
    @Override
    public boolean isDirectory( String fileSystem, String path )
    {
        if ( path.endsWith( "/" ) )
        {
            return true;
        }
        String parentPath = getParentPath( path );
        String filename = getFilename( path ) + "/";

        BoundStatement bound = preparedSingleExistQuery.bind( fileSystem, parentPath, filename );
        ResultSet result = session.execute( bound );
        long count = result.one().get( 0, Long.class );
        return count > 0;
    }

    @Override
    public boolean isFile( String fileSystem, String path )
    {
        if ( path.endsWith( "/" ) )
        {
            return false;
        }
        String parentPath = getParentPath( path );
        String filename = getFilename( path );

        BoundStatement bound = preparedSingleExistQuery.bind( fileSystem, parentPath, filename );
        ResultSet result = session.execute( bound );
        long count = result.one().get( 0, Long.class );
        return count > 0;
    }

    @Override
    public boolean delete( String fileSystem, String path )
    {
        PathMap pathMap = getPathMap( fileSystem, path );
        if ( pathMap == null )
        {
            logger.debug( "File not exists, {}", pathMap );
            return true;
        }

        String fileId = pathMap.getFileId();
        if ( fileId == null )
        {
            logger.debug( "Can not delete a directory, {}", pathMap );
            return false;
        }

        logger.debug( "Delete pathMap, {}", pathMap );
        pathMapMapper.delete( pathMap.getFileSystem(), pathMap.getParentPath(), pathMap.getFilename() );


        ReverseMap reverseMap = deleteFromReverseMap( pathMap.getFileId(), marshall( fileSystem, path ) );
        logger.debug( "Updated reverseMap, {}", reverseMap );

        // need to check store checksum first to decide if
        if ( reverseMap == null || reverseMap.getPaths() == null || reverseMap.getPaths().isEmpty() )
        {
            // clean checksum in checksum table as no file id refer to it.
            FileChecksum checksum = fileChecksumMapper.get( pathMap.getChecksum() );
            if ( checksum != null )
            {
                // update file checksum
                logger.debug( "Delete file checksum, {}", checksum );
                fileChecksumMapper.delete( checksum.getChecksum() );
            }

            // reclaim, but not remove from reverse table immediately (for race-detection/double-check)
            reclaim( fileId, pathMap.getFileStorage() );

        }
        return true;
    }

    // We have to use non-prepared statement because bind variables are not supported inside collections
    private ReverseMap deleteFromReverseMap( String fileId, String path )
    {
        logger.debug( "Delete from reverseMap, fileId: {}, path: {}", fileId, path );
        session.execute( "UPDATE " + keyspace + ".reversemap SET paths = paths - {'" + path + "'} WHERE fileid=?;", fileId );
        return reverseMapMapper.get( fileId );
    }

    private void addToReverseMap( String fileId, String path )
    {
        logger.debug( "Add to reverseMap, fileId: {}, path: {}", fileId, path );
        session.execute( "UPDATE " + keyspace + ".reversemap SET paths = paths + {'" + path + "'} WHERE fileid=?;", fileId );
    }

    private void reclaim( String fileId, String fileStorage )
    {
        DtxReclaim reclaim = new DtxReclaim( fileId, new Date(), fileStorage );
        logger.debug( "Reclaim, {}", reclaim );
        reclaimMapper.save( reclaim );
    }

    @Override
    public String getStorageFile( String fileSystem, String path )
    {
        PathMap pathMap = getPathMap( fileSystem, path );
        if ( pathMap != null )
        {
            return pathMap.getFileStorage();
        }
        return null;
    }

    @Override
    public boolean copy( String fromFileSystem, String fromPath, String toFileSystem, String toPath )
    {
        PathMap pathMap = getPathMap( fromFileSystem, fromPath );
        if ( pathMap == null )
        {
            logger.warn( "Source not found, {}:{}", fromFileSystem, fromPath );
            return false;
        }

        PathMap target = getPathMap( toFileSystem, toPath );
        if ( target != null )
        {
            logger.info( "Target already exists, delete it. {}:{}", toFileSystem, toPath );
            delete( toFileSystem, toPath );
        }

        // check parent paths
        String fromParentPath = getParentPath( fromPath );
        String toParentPath = getParentPath( toPath );
        if ( fromParentPath != null && !fromParentPath.equals( toParentPath ) )
        {
            makeDirs( toFileSystem, toParentPath );
        }

        String toFilename = getFilename( toPath );
        pathMapMapper.save( new DtxPathMap( toFileSystem, toParentPath, toFilename, pathMap.getFileId(),
                                            pathMap.getCreation(), pathMap.getSize(), pathMap.getFileStorage(), pathMap.getChecksum() ) );
        return true;
    }

    @Override
    public void makeDirs( String fileSystem, String path )
    {
        logger.debug( "Make dir, fileSystem: {}, path: {}", fileSystem, path );

        if ( ROOT_DIR.equals( path ) )
        {
            return;
        }
        if ( !path.endsWith( "/" ) )
        {
            path += "/";
        }

        String parentPath = getParentPath( path );
        String filename = getFilename( path );

        BoundStatement bound = preparedSingleExistQuery.bind( fileSystem, parentPath, filename );
        ResultSet result = session.execute( bound );
        long count = result.one().get( 0, Long.class );
        if ( count > 0 )
        {
            logger.debug( "Dir already exists, fileSystem: {}, path: {}", fileSystem, path );
            return;
        }

        DtxPathMap pathMap = new DtxPathMap();
        pathMap.setFileSystem( fileSystem );
        pathMap.setParentPath( parentPath );
        pathMap.setFilename( filename );

        final List<DtxPathMap> parents = getParentsBottomUp( pathMap, ( fSystem, pPath, fName ) -> {
            DtxPathMap p = new DtxPathMap();
            p.setFileSystem( fSystem );
            p.setParentPath( pPath );
            p.setFilename( fName );
            return p;
        } );

        List<DtxPathMap> persist = new ArrayList<>();
        persist.add( pathMap );
        persist.addAll( parents );

        logger.debug( "Persist: {}", persist );
        persist.forEach( e -> pathMapMapper.save( e ) );
    }

    @Override
    public List<Reclaim> listOrphanedFiles( int limit )
    {
        // timestamp data type is encoded as the number of milliseconds since epoch
        Date cur = new Date();
        long threshold = getReclaimThreshold( cur, config.getGCGracePeriodInHours() );
        logger.debug( "listOrphanedFiles, cur: {}, threshold: {}, limit: {}", cur, new Date( threshold ), limit );
        ResultSet result;
        String baseQuery = "SELECT * FROM " + keyspace + ".reclaim WHERE partition = 0 AND deletion < ?";
        if ( limit > 0 )
        {
            result = session.execute( baseQuery + " limit ?;", threshold, limit );
        }
        else
        {
            result = session.execute( baseQuery + ";", threshold );
        }
        Result<DtxReclaim> ret = reclaimMapper.map( result );
        return new ArrayList<>( ret.all() );
    }

    @Override
    public void removeFromReclaim( Reclaim reclaim )
    {
        reclaimMapper.delete( (DtxReclaim) reclaim );
    }

    private long getReclaimThreshold( Date date, int gcGracePeriodInHours )
    {
        long ret = date.getTime();
        if ( gcGracePeriodInHours <= 0 )
        {
            return ret;
        }
        return ret - Duration.ofHours( gcGracePeriodInHours ).toMillis();
    }

}
