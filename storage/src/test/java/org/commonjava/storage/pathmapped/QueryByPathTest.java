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

import org.commonjava.storage.pathmapped.pathdb.datastax.model.DtxPathMap;
import org.commonjava.storage.pathmapped.pathdb.datastax.model.DtxReverseMap;
import org.commonjava.storage.pathmapped.util.PathMapUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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

        // check the reverse mapping for repo1/path1
        String parentPath = PathMapUtils.getParentPath( path1 );
        String filename = PathMapUtils.getFilename( path1 );
        DtxPathMap pathMap = pathMapMapper.get( repo1, parentPath, filename );
        String fileId = pathMap.getFileId();
        DtxReverseMap reverseMap = reverseMapMapper.get( fileId );
        assertNotNull( reverseMap );
        ret = reverseMap.getPaths();
        System.out.println(">>> " + ret );
        assertTrue( ret.contains( repo1 + ":" + path1 ) );

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

        // check the reverse mapping is null
        reverseMap = reverseMapMapper.get( fileId );
        assertNull( reverseMap );
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

        // check the reverse mapping paths contain both repo1 and repo2
        String parentPath = PathMapUtils.getParentPath( path1 );
        String filename = PathMapUtils.getFilename( path1 );
        DtxPathMap pathMap = pathMapMapper.get( from, parentPath, filename );
        DtxReverseMap reverseMap = reverseMapMapper.get( pathMap.getFileId() );
        ret = reverseMap.getPaths();
        System.out.println(">>> " + ret );
        assertTrue( ret.contains( repo1 + ":" + path1 ) );
        assertTrue( ret.contains( repo2 + ":" + path1 ) );
    }

}
