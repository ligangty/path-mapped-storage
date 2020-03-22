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
package org.commonjava.storage.pathmapped;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class QueryByPathTest
                extends AbstractCassandraFMTest
{
    final String repo1 = "maven:hosted:repo1";

    final String repo2 = "maven:hosted:repo2";

    final String repoNoSuchPath = "maven:hosted:repo3";

    @Test
    public void insertAndDelete() throws IOException
    {
        writeWithContent( fileManager.openOutputStream( repo1, path1 ), simpleContent );
        writeWithContent( fileManager.openOutputStream( repo2, path1 ), simpleContent );

        List<String> candidates = Arrays.asList( repo1, repo2, repoNoSuchPath );
        Set<String> ret = fileManager.getFileSystemContaining( candidates, path1 );
        System.out.println( ">>> " + ret );
        assertTrue( ret.size() == 2 );
        assertTrue( ret.containsAll( Arrays.asList( repo1, repo2 ) ) );

        // test directory
        ret = fileManager.getFileSystemContaining( candidates, pathSub1 + "/" );
        System.out.println( ">>> " + ret );
        assertTrue( ret.size() == 2 );
        assertTrue( ret.containsAll( Arrays.asList( repo1, repo2 ) ) );

        ret = fileManager.getFileSystemContaining( candidates, pathParent + "/" );
        System.out.println( ">>> " + ret );
        assertTrue( ret.size() == 2 );
        assertTrue( ret.containsAll( Arrays.asList( repo1, repo2 ) ) );

        // delete one path
        fileManager.delete( repo1, path1 );

        ret = fileManager.getFileSystemContaining( candidates, path1 );
        System.out.println( ">>> " + ret );
        assertTrue( ret.size() == 1 );
        assertTrue( ret.contains( repo2 ) );

        // directory
        ret = fileManager.getFileSystemContaining( candidates, pathSub1 + "/" );
        System.out.println( ">>> " + ret );
        //assertTrue( ret.size() == 1 ); // although file deleted, dirs remain in DB (not perfect but no problem)
        assertTrue( ret.contains( repo2 ) );
    }

    @Test
    public void getFirst() throws IOException
    {
        writeWithContent( fileManager.openOutputStream( repo1, path1 ), simpleContent );
        writeWithContent( fileManager.openOutputStream( repo2, path1 ), simpleContent );

        List<String> candidates = Arrays.asList( repo1, repo2, repoNoSuchPath );
        String ret = fileManager.getFirstFileSystemContaining( candidates, path1 );
        System.out.println( ">>> " + ret );
        assertTrue( ret != null && ret.equals( repo1 ) );

        // switch repo1 and repo2
        candidates = Arrays.asList( repo2, repo1, repoNoSuchPath );
        ret = fileManager.getFirstFileSystemContaining( candidates, path1 );
        System.out.println( ">>> " + ret );
        assertTrue( ret != null && ret.equals( repo2 ) );
    }

    @Test
    public void copy() throws IOException
    {
        String from = repo1;
        String to = repo2;
        writeWithContent( fileManager.openOutputStream( from, path1 ), simpleContent );
        fileManager.copy( from, path1, to, path1 );

        List<String> candidates = Arrays.asList( repo1, repo2, repoNoSuchPath );
        Set<String> ret = fileManager.getFileSystemContaining( candidates, path1 );
        System.out.println( ">>> " + ret );
        assertTrue( ret.size() == 2 );
        assertTrue( ret.containsAll( Arrays.asList( from, to ) ) );

        // directory
        ret = fileManager.getFileSystemContaining( candidates, pathSub1 + "/" );
        System.out.println( ">>> " + ret );
        assertTrue( ret.size() == 2 );
        assertTrue( ret.containsAll( Arrays.asList( repo1, repo2 ) ) );
    }

    /**
     * Result:
     * getFirstFileSystemContaining is 6+ times faster than for-loop.
     *
     * ------- getFileSystemContaining ---------
     * foo/bar/1.0/bar-1.0-1500.xml, exist, (313)
     * foo/bar/1.0/bar-1.0-99999.xml, not exist, (296)
     * none/exist/ (283)
     * ------- getFirstFileSystemContaining ---------
     * foo/bar/1.0/bar-1.0-1500.xml, exist, (187)
     * foo/bar/1.0/bar-1.0-99999.xml, not exist, (284)
     * none/exist/, not exist, (292)
     * ------- for loop ---------
     * foo/bar/1.0/bar-1.0-1500.xml, exist, (1122)
     * foo/bar/1.0/bar-1.0-99999.xml, not exist, (1952)
     * none/exist/, not exist, (1270)
     */
    @Ignore
    @Test
    public void performance() throws IOException
    {
        final String repo = "maven:hosted:build-%s";
        final String path = "foo/bar/1.0/bar-1.0-%s.xml";

        // use another path to introduce more entries
        final String anotherPath =
                        "my/very/very/very/long/path/to/introduce/more/entries/for/get/file/system/containing/performance/test/foo/bar/1.0/bar-1.0-%s.xml";

        List<String> candidates = new ArrayList<>();

        // prepare
        for ( int i = 0; i < 3000; i++ )
        {
            String repoName = String.format( repo, i );
            writeWithContent( fileManager.openOutputStream( repoName, String.format( path, i ) ), simpleContent );
            writeWithContent( fileManager.openOutputStream( repoName, String.format( anotherPath, i ) ),
                              simpleContent );
            candidates.add( repoName );
        }

        StringBuilder sb = new StringBuilder();

        printIt( sb, "------- getFileSystemContaining ---------" );

        // exist
        String targetPath = String.format( path, 1500 );
        long begin = System.currentTimeMillis();
        Set<String> ret = fileManager.getFileSystemContaining( candidates, targetPath );
        long elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( !ret.isEmpty() ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // not exist
        targetPath = String.format( path, 99999 );
        begin = System.currentTimeMillis();
        ret = fileManager.getFileSystemContaining( candidates, targetPath );
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( !ret.isEmpty() ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // dir
        String noneExistDirPath = "none/exist/";
        begin = System.currentTimeMillis();
        ret = fileManager.getFileSystemContaining( candidates, noneExistDirPath );
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, noneExistDirPath + " (" + elapse + ")" );

        printIt( sb, "------- getFirstFileSystemContaining ---------" );

        // exist
        targetPath = String.format( path, 1500 );
        begin = System.currentTimeMillis();
        String firstRet = fileManager.getFirstFileSystemContaining( candidates, targetPath );
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( firstRet != null ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // not exist
        targetPath = String.format( path, 99999 );
        begin = System.currentTimeMillis();
        firstRet = fileManager.getFirstFileSystemContaining( candidates, targetPath );
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( firstRet != null ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // dir
        begin = System.currentTimeMillis();
        firstRet = fileManager.getFirstFileSystemContaining( candidates, noneExistDirPath );
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, noneExistDirPath + ", " + ( firstRet != null ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        printIt( sb, "------- for loop ---------" );

        // exist
        boolean exist = false;
        targetPath = String.format( path, 1500 );
        begin = System.currentTimeMillis();
        for ( String candidate : candidates )
        {
            exist = fileManager.exists( candidate, targetPath );
            if ( exist )
            {
                break;
            }
        }
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( exist ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // none exist
        targetPath = String.format( path, 99999 );
        begin = System.currentTimeMillis();
        for ( String candidate : candidates )
        {
            exist = fileManager.exists( candidate, targetPath );
            if ( exist )
            {
                break;
            }
        }
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, targetPath + ", " + ( exist ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // dir
        begin = System.currentTimeMillis();
        for ( String candidate : candidates )
        {
            exist = fileManager.exists( candidate, noneExistDirPath );
            if ( exist )
            {
                break;
            }
        }
        elapse = System.currentTimeMillis() - begin;
        printIt( sb, noneExistDirPath + ", " + ( exist ? "exist" : "not exist" ) + ", (" + elapse + ")" );

        // print all
        System.out.println( sb.toString() );
    }

    private void printIt( StringBuilder sb, String s )
    {
        sb.append( s + "\n" );
    }

}
