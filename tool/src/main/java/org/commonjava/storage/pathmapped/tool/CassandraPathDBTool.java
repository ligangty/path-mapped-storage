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
import org.commonjava.storage.pathmapped.pathdb.datastax.CassandraPathDB;
import org.commonjava.storage.pathmapped.pathdb.datastax.model.DtxPathMap;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.commons.lang.StringUtils.isNotBlank;

public class CassandraPathDBTool
{
    private final Session session;

    private final String filesystem;

    private final PreparedStatement preparedListQuery;

    private Mapper<DtxPathMap> pathMapMapper;

    private final CassandraPathDB cassandraPathDB;

    public CassandraPathDBTool( Session session, String keyspace, String filesystem )
    {
        this.session = session;
        this.filesystem = filesystem;

        preparedListQuery = session.prepare(
                        "SELECT * FROM " + keyspace + ".pathmap WHERE filesystem=? and parentpath=?;" );

        MappingManager manager = new MappingManager( session );
        pathMapMapper = manager.mapper( DtxPathMap.class, keyspace );

        cassandraPathDB = new CassandraPathDB( new DefaultPathMappedStorageConfig(), session, keyspace );
    }

    public void get( String absolutePath, Consumer<DtxPathMap> consumer )
    {
        consumer.accept( cassandraPathDB.getPathMap( filesystem, absolutePath ) );
    }

    public boolean delete( String absolutePath )
    {
        return cassandraPathDB.delete( filesystem, absolutePath );
    }

    private static class PathAndType
    {
        enum Type
        {
            FILE, DIR
        }

        String path;

        Type type;

        public PathAndType( String path, Type type )
        {
            this.path = path;
            this.type = type;
        }
    }

    public void traverse( String rootPath, String listType, Consumer<String> consumer )
    {
        TreeTraverser<PathAndType> traverser = new TreeTraverser<PathAndType>()
        {
            @Override
            public Iterable<PathAndType> children( PathAndType pnt )
            {
                String parentPath = pnt.path;
                List<PathAndType> children = new ArrayList<>();
                BoundStatement bound = preparedListQuery.bind( filesystem, parentPath );
                ResultSet result = session.execute( bound );
                pathMapMapper.map( result ).all().forEach( dtxPathMap -> {
                    String path = Paths.get( dtxPathMap.getParentPath(), dtxPathMap.getFilename() ).toString();
                    PathAndType child;
                    if ( dtxPathMap.getFileId() == null )
                    {
                        child = new PathAndType( path, PathAndType.Type.DIR );
                    }
                    else
                    {
                        child = new PathAndType( path, PathAndType.Type.FILE );
                    }
                    children.add( child );
                } );
                return children;
            }
        };
        traverser.preOrderTraversal( new PathAndType( rootPath, PathAndType.Type.DIR ) ).forEach( pnt -> {
            if ( listType.equals( "all" ) || pnt.type.name().equalsIgnoreCase( listType ) )
            {
                consumer.accept( pnt.path );
            }
        } );
    }

    public static void main( String[] args )
    {
        Options options = new Options();

        Option optHost = new Option( "h", "host", true, "host (default localhost)" );
        options.addOption( optHost );

        Option optPort = new Option( "p", "port", true, "port (default 9042)" );
        options.addOption( optPort );

        Option optUser = new Option( "u", "user", true, "username" );
        options.addOption( optUser );

        Option optPass = new Option( "P", "pass", true, "password" );
        options.addOption( optPass );


        OptionGroup operationalOptions = new OptionGroup();

        Option optList = new Option( "l", "list", true, "list specified path" );
        optList.setOptionalArg( true );
        operationalOptions.addOption( optList );

        Option optDelete = new Option( "d", "delete", true, "delete specified file" );
        operationalOptions.addOption( optDelete );

/*
        Option optGet = new Option( null, "get", true, "get specified file" );
        operationalOptions.addOption( optGet );
*/

        Option optInfo = new Option( "i", "info", true, "get specified path info" );
        operationalOptions.addOption( optInfo );

        options.addOptionGroup( operationalOptions );

        Option optKeyspace = new Option( "k", "keyspace", true, "keyspace" );
        optKeyspace.setRequired( true );
        options.addOption( optKeyspace );

        Option optFilesystem = new Option( "f", "filesystem", true, "filesystem" );
        optFilesystem.setRequired( true );
        options.addOption( optFilesystem );

        Option optType = new Option( "t", "type", true, "filter list result by type (file/dir)" );
        options.addOption( optType );

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

        CassandraPathDBTool pathDBTool = new CassandraPathDBTool( session, keyspace, filesystem );

        if ( cmd.hasOption( optInfo.getOpt() ) )
        {
            String getPath = cmd.getOptionValue( optInfo.getOpt() );
            pathDBTool.get( getPath, o -> System.out.println( o ) );
        }
/*
        else if ( cmd.hasOption( optGet.getLongOpt() ) )
        {
            String getPath = cmd.getOptionValue( optGet.getLongOpt() );
            viewer.get( getPath, o -> System.out.println( o ) );
        }
*/
        else if ( cmd.hasOption( optDelete.getOpt() ) )
        {
            String deletePath = cmd.getOptionValue( optDelete.getOpt() );
            boolean deleted = pathDBTool.delete( deletePath );
            System.out.println( "Deleted: " + deleted );
        }
        else if ( cmd.hasOption( optList.getOpt() ) )
        {
            String listRoot = cmd.getOptionValue( optList.getOpt(), "/" );
            String listType = cmd.getOptionValue( optType.getOpt(), "all" );
            System.out.println( "$ ls -l " + listRoot + " -t " + listType );
            pathDBTool.traverse( listRoot, listType, s -> System.out.println( s ) );
        }

        session.close();
        cluster.close();
    }

}
