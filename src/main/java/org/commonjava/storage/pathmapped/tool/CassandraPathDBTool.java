package org.commonjava.storage.pathmapped.tool;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.TreeTraverser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.commonjava.storage.pathmapped.config.DefaultPathMappedStorageConfig;
import org.commonjava.storage.pathmapped.datastax.CassandraPathDB;
import org.commonjava.storage.pathmapped.datastax.model.DtxPathMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.commons.lang.StringUtils.isNotBlank;

public class CassandraPathDBTool
{
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final Session session;

    private final String keyspace;

    private final String filesystem;

    private final PreparedStatement preparedListQuery;

    private Mapper<DtxPathMap> pathMapMapper;

    private final CassandraPathDB cassandraPathDB;

    public CassandraPathDBTool( Session session, String keyspace, String filesystem )
    {
        this.session = session;
        this.keyspace = keyspace;
        this.filesystem = filesystem;

        preparedListQuery = session.prepare(
                        "SELECT * FROM " + keyspace + ".pathmap WHERE filesystem=? and parentpath=?;" );

        MappingManager manager = new MappingManager( session );
        pathMapMapper = manager.mapper( DtxPathMap.class, keyspace );

        cassandraPathDB = new CassandraPathDB( new DefaultPathMappedStorageConfig(), session, filesystem );
    }

    public void get( String absolutePath, Consumer<DtxPathMap> consumer )
    {
        consumer.accept( cassandraPathDB.getPathMap( filesystem, absolutePath ) );
    }

    public boolean delete( String absolutePath )
    {
        return cassandraPathDB.delete( filesystem, absolutePath );
    }

    public void traverse( String rootPath, Consumer<String> consumer )
    {
        TreeTraverser<String> traverser = new TreeTraverser<String>()
        {
            @Override
            public Iterable<String> children( String s )
            {
                String parentPath = s;
                List<String> children = new ArrayList<>();
                BoundStatement bound = preparedListQuery.bind( filesystem, parentPath );
                ResultSet result = session.execute( bound );
                pathMapMapper.map( result ).all().forEach( dtxPathMap -> {
                    String path = Paths.get( dtxPathMap.getParentPath(), dtxPathMap.getFilename() ).toString();
                    children.add( path );
                } );
                return children;
            }
        };
        traverser.preOrderTraversal( rootPath ).forEach( s -> consumer.accept( s ) );
    }


    public static void main( String[] args )
    {
        Options options = new Options();

        OptionGroup connectOptions = new OptionGroup();

        Option optHost = new Option( "h", "host", true, "host (default localhost)" );
        connectOptions.addOption( optHost );

        Option optPort = new Option( "p", "port", true, "port (default 9042)" );
        connectOptions.addOption( optPort );

        Option optUser = new Option( "u", "user", true, "username" );
        connectOptions.addOption( optUser );

        Option optPass = new Option( "P", "pass", true, "password" );
        connectOptions.addOption( optPass );

        options.addOptionGroup( connectOptions );

        OptionGroup operationalOptions = new OptionGroup();

        Option optList = new Option( "l", "list", true, "list specified path" );
        optList.setOptionalArg( true );
        operationalOptions.addOption( optList );

        Option optDelete = new Option( "d", "delete", true, "delete specified file" );
        operationalOptions.addOption( optDelete );

        Option optGet = new Option( null, "get", true, "get specified file" );
        operationalOptions.addOption( optGet );

        options.addOptionGroup( operationalOptions );

        Option optKeyspace = new Option( "k", "keyspace", true, "keyspace" );
        optKeyspace.setRequired( true );
        options.addOption( optKeyspace );

        Option optFilesystem = new Option( "f", "filesystem", true, "filesystem" );
        optFilesystem.setRequired( true );
        options.addOption( optFilesystem );

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try
        {
            cmd = parser.parse( options, args );
        }
        catch ( ParseException e )
        {
            System.out.println( e.getMessage() );
            formatter.printHelp( "pathDB", options );
            System.exit( 1 );
        }

        String host = cmd.getOptionValue( optHost.getOpt(), "localhost" );
        int port = Integer.parseInt( cmd.getOptionValue( optPort.getOpt(), "9042" ) );
        String keyspace = cmd.getOptionValue( optKeyspace.getOpt() );
        String filesystem = cmd.getOptionValue( optFilesystem.getOpt() );
        String username = cmd.getOptionValue( optUser.getOpt() );
        String password = cmd.getOptionValue( optUser.getOpt() );

        Cluster.Builder builder = Cluster.builder().withoutJMXReporting().addContactPoint( host ).withPort( port );
        if ( isNotBlank( username ) && isNotBlank( password ) )
        {
            builder.withCredentials( username, password );
        }
        Cluster cluster = builder.build();
        Session session = cluster.connect();

        CassandraPathDBTool viewer = new CassandraPathDBTool( session, keyspace, filesystem );

        if ( cmd.hasOption( optGet.getLongOpt() ) )
        {
            String getPath = cmd.getOptionValue( optGet.getLongOpt() );
            viewer.get( getPath, o -> System.out.println( o ) );
        }
        else if ( cmd.hasOption( optDelete.getOpt() ) )
        {
            String deletePath = cmd.getOptionValue( optDelete.getOpt() );
            boolean deleted = viewer.delete( deletePath );
            System.out.println( "Deleted: " + deleted );
        }
        else if ( cmd.hasOption( optList.getOpt() ) )
        {
            String listRoot = cmd.getOptionValue( optList.getOpt(), "/" );
            System.out.println( "$ ls -l " + listRoot );
            viewer.traverse( listRoot, s -> System.out.println( s ) );
        }

        session.close();
        cluster.close();
    }

}
