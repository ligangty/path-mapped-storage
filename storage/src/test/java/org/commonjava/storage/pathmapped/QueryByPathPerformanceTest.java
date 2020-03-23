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
import java.util.List;
import java.util.Set;

@Ignore
public class QueryByPathPerformanceTest
                extends AbstractCassandraFMTest
{

    /**
     * We set 3000 file systems each has a different path. And we do:
     * 1. search for the path of the 1500th file system
     * 2. search for a none existing path
     * 3. search for a none existing directory
     *
     * Result:
     * 1. getFileSystemContaining is 4+ times faster than for-loop (317 vs 1340)
     * 2. getFileSystemContaining is 7+ times faster than for-loop (252 vs 1962)
     * 3. getFileSystemContaining is 4+ times faster than for-loop (311 vs 1311)
     *
     * getFirstFileSystemContaining is almost the same as getFileSystemContaining. If the candidates are many, it will
     * split them to batches. If the target is near the starting end of candidates, this api is faster (186 vs 317).
     * Otherwise it is a little slower (304 vs 252). For smaller candidates, it is same to getFileSystemContaining.
     *
     * Sample output:
     *
     * ------- getFileSystemContaining ---------
     * foo/bar/1.0/bar-1.0-1500.xml, exist, (317)
     * foo/bar/1.0/bar-1.0-99999.xml, not exist, (252)
     * none/exist/ (311)
     * ------- getFirstFileSystemContaining ---------
     * foo/bar/1.0/bar-1.0-1500.xml, exist, (186)
     * foo/bar/1.0/bar-1.0-99999.xml, not exist, (304)
     * none/exist/, not exist, (292)
     * ------- for loop ---------
     * foo/bar/1.0/bar-1.0-1500.xml, exist, (1340)
     * foo/bar/1.0/bar-1.0-99999.xml, not exist, (1962)
     * none/exist/, not exist, (1311)
     */
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
        ret = fileManager.getFileSystemContainingDirectory( candidates, noneExistDirPath );
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
