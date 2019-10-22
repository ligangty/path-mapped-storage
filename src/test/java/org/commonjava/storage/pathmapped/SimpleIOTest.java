/**
 * Copyright (C) 2013~2019 Red Hat, Inc.
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
package org.commonjava.storage.pathmapped;

import ch.qos.logback.core.util.FileSize;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class SimpleIOTest
        extends AbstractCassandraFMTest
{

    @Test
    public void readWrittenFile()
            throws Exception
    {
        File f = temp.newFile( "read-target.txt" );
        String src = "This is a test";

        try (OutputStream os = fileManager.openOutputStream( TEST_FS, f.getPath() ))
        {
            assertNotNull( os );
            IOUtils.write( src.getBytes(), os );
        }

        try (InputStream is = fileManager.openInputStream( TEST_FS, f.getPath() ))
        {
            assertNotNull( is );
            String result = new String( IOUtils.toByteArray( is ), Charset.defaultCharset() );
            assertThat( result, equalTo( src ) );
        }
    }

    @Test
    public void read1MbFile()
            throws Exception
    {
        assertReadFileOfSize( "1mb" );
    }

    @Test
    public void read11MbFile()
            throws Exception
    {
        assertReadFileOfSize( "11mb" );
    }

    private void assertReadFileOfSize( String s )
            throws IOException
    {
        File f = temp.newFile( "read-target.txt" );
        int sz = (int) FileSize.valueOf( s ).getSize();
        byte[] src = new byte[sz];

        Random rand = new Random();
        rand.nextBytes( src );

        try (OutputStream out = fileManager.openOutputStream( TEST_FS, f.getPath() ))
        {
            IOUtils.write( src, out );
        }

        try (InputStream stream = fileManager.openInputStream( TEST_FS, f.getPath() ))
        {
            byte[] result = IOUtils.toByteArray( stream );
            assertThat( result, equalTo( src ) );
        }
    }

    @Test
    public void writeToFileWithLines()
            throws Exception
    {
        final File tempFile = temp.newFile();

        writeWithCount( fileManager.openOutputStream( TEST_FS, tempFile.getPath() ) );

        final File file = new File( tempFile.getPath() );
        System.out.println( "File length: " + file.length() );
        try (InputStream is = fileManager.openInputStream( TEST_FS, tempFile.getPath() ))
        {
            final List<String> lines = IOUtils.readLines( is );
            System.out.println( lines );
            assertThat( lines.size(), equalTo( COUNT ) );
        }

    }

    @Test
    public void overwriteFile()
            throws Exception
    {
        File f = temp.newFile();
        try (OutputStream stream = fileManager.openOutputStream( TEST_FS, f.getPath() ))
        {
            String longer = "This is a really really really long string";
            stream.write( longer.getBytes() );
        }

        String shorter = "This is a short string";
        try (OutputStream stream = fileManager.openOutputStream( TEST_FS, f.getPath() ))
        {
            stream.write( shorter.getBytes() );
        }

        long fileLength = fileManager.getFileLength( TEST_FS, f.getPath() );
        System.out.println( "File length: " + fileLength );
        assertThat( fileLength, equalTo( (long) shorter.getBytes().length ) );

        try (InputStream stream = fileManager.openInputStream( TEST_FS, f.getPath() ))
        {
            String content = IOUtils.toString( stream );
            assertThat( content, equalTo( shorter ) );
        }

    }

    @Test
    public void fileRepeatedRead()
            throws Exception
    {
        final File f = temp.newFile();
        String str = "This is a test";
        writeWithContent( fileManager.openOutputStream( TEST_FS, f.getPath() ), str );
        InputStream s1 = null;
        InputStream s2 = null;

        Logger logger = LoggerFactory.getLogger( getClass() );
        try
        {

            s1 = fileManager.openInputStream( TEST_FS, f.getPath() );
            s2 = fileManager.openInputStream( TEST_FS, f.getPath() );

            logger.info( "READ first " );
            String out1 = IOUtils.toString( s1 );
            logger.info( "READ second " );
            String out2 = IOUtils.toString( s2 );

            assertThat( "first reader returned wrong data", out1, equalTo( str ) );
            assertThat( "second reader returned wrong data", out2, equalTo( str ) );
        }
        finally
        {
            logger.info( "CLOSE first thread" );
            IOUtils.closeQuietly( s1 );
            logger.info( "CLOSE second thread" );
            IOUtils.closeQuietly( s2 );
        }

    }

    @Test
    public void deleteFile()
            throws IOException
    {
        final File f = temp.newFile();
        String str = "This is a test";
        try (InputStream is = fileManager.openInputStream( TEST_FS, f.getPath() ))
        {
            assertThat( is, nullValue() );
        }
        writeWithContent( fileManager.openOutputStream( TEST_FS, f.getPath() ), str );
        try (InputStream is = fileManager.openInputStream( TEST_FS, f.getPath() ))
        {
            assertThat( is, notNullValue() );
        }
        assertThat( fileManager.delete( TEST_FS, f.getPath() ), equalTo( true ) );
        try (InputStream is = fileManager.openInputStream( TEST_FS, f.getPath() ))
        {
            assertThat( is, nullValue() );
        }
    }

    @Test
    public void existsFile()
            throws IOException
    {
        final File f = temp.newFile();
        String str = "This is a test";
        assertThat( fileManager.exists( TEST_FS, f.getPath() ), equalTo( false ) );
        assertThat( fileManager.exists( TEST_FS, f.getPath() + "/" ), equalTo( false ) );
        assertThat( fileManager.exists( TEST_FS, f.getParent() ), equalTo( false ) );
        assertThat( fileManager.exists( TEST_FS, f.getParent() + "/" ), equalTo( false ) );
        writeWithContent( fileManager.openOutputStream( TEST_FS, f.getPath() ), str );
        assertThat( fileManager.exists( TEST_FS, f.getPath() ), equalTo( true ) );
        assertThat( fileManager.exists( TEST_FS, f.getPath() + "/" ), equalTo( false ) );
        assertThat( fileManager.exists( TEST_FS, f.getParent() ), equalTo( true ) );
        assertThat( fileManager.exists( TEST_FS, f.getParent() + "/" ), equalTo( true ) );
    }

    @Test
    public void fileAndDirectory()
            throws IOException
    {
        final File f = temp.newFile();
        String str = "This is a test";
        assertThat( fileManager.isFile( TEST_FS, f.getPath() ), equalTo( false ) );
        assertThat( fileManager.isDirectory( TEST_FS, f.getPath() ), equalTo( false ) );
        assertThat( fileManager.isDirectory( TEST_FS, f.getPath() + "/" ), equalTo( false ) );
        assertThat( fileManager.isDirectory( TEST_FS, f.getParent() ), equalTo( false ) );
        assertThat( fileManager.isDirectory( TEST_FS, f.getParent() + "/" ), equalTo( false ) );
        writeWithContent( fileManager.openOutputStream( TEST_FS, f.getPath() ), str );
        assertThat( fileManager.isFile( TEST_FS, f.getPath() ), equalTo( true ) );
        assertThat( fileManager.isDirectory( TEST_FS, f.getPath() ), equalTo( false ) );
        assertThat( fileManager.isDirectory( TEST_FS, f.getPath() + "/" ), equalTo( false ) );
        assertThat( fileManager.isDirectory( TEST_FS, f.getParent() ), equalTo( true ) );
        assertThat( fileManager.isDirectory( TEST_FS, f.getParent() + "/" ), equalTo( true ) );
    }

    @Test
    public void listEntriesInSameFolder()
            throws IOException
    {
        File f1 = temp.newFile( "f1.txt" );
        File f2 = temp.newFile( "f2.txt" );
        writeWithContent( fileManager.openOutputStream( TEST_FS, f1.getPath() ), "This is test f1" );
        writeWithContent( fileManager.openOutputStream( TEST_FS, f2.getPath() ), "This is test f2" );
        List<String> lists = Arrays.asList( fileManager.list( TEST_FS, temp.getRoot().getPath() ) );
        assertThat( lists, hasItems( "f1.txt", "f2.txt" ) );

        Path rootPath = Paths.get( temp.getRoot().toURI() );
        lists = Arrays.asList( fileManager.list( TEST_FS, temp.getRoot().getParent() ) );
        assertThat( lists, hasItems( rootPath.getName( rootPath.getNameCount() - 1 ).toString() + "/" ) );
    }

    @Test
    public void listEntriesInDiffFolders()
            throws IOException
    {
        final File folder1 = temp.newFolder();
        final File folder2 = temp.newFolder();
        final Path folder1Path = Paths.get( folder1.toURI() );
        final Path folder2Path = Paths.get( folder2.toURI() );
        File f1 = folder1Path.resolve( "f1.txt" ).toFile();
        File f2 = folder2Path.resolve( "f2.txt" ).toFile();
        writeWithContent( fileManager.openOutputStream( TEST_FS, f1.getPath() ), "This is test f1" );
        writeWithContent( fileManager.openOutputStream( TEST_FS, f2.getPath() ), "This is test f2" );
        List<String> lists = Arrays.asList( fileManager.list( TEST_FS, temp.getRoot().getPath() ) );
        assertThat( lists, hasItems( folder1Path.getName( folder1Path.getNameCount() - 1 ).toString() + "/",
                                     folder2Path.getName( folder2Path.getNameCount() - 1 ).toString() + "/" ) );

        lists = Arrays.asList( fileManager.list( TEST_FS, folder1.getPath() ) );
        assertThat( lists, hasItems( "f1.txt" ) );
        lists = Arrays.asList( fileManager.list( TEST_FS, folder2.getPath() ) );
        assertThat( lists, hasItems( "f2.txt" ) );
    }

    private void writeWithCount( OutputStream stream )
    {
        try (OutputStream os = stream)
        {
            for ( int i = 0; i < COUNT; i++ )
            {
                os.write( String.format( "%d\n", i ).getBytes() );
            }

        }
        catch ( final IOException e )
        {
            e.printStackTrace();
        }
    }

    private void writeWithContent( OutputStream stream, String content )
    {
        try (OutputStream os = stream)
        {
            IOUtils.write( content.getBytes(), os );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }
}
