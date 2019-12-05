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

import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class CassandraPathDBToolTest extends AbstractCassandraTest
{
    private String content = "This is a test";

    private String ext = ".http-metadata.json";

    @Test
    public void listAndDelete() throws IOException
    {
        String path1 = "org/foo/bar/1.0/bar-1.0.jar";

        writeWithContent( TEST_FS, path1, content );
        writeWithContent( TEST_FS, path1 + ext, content );

        String path2 = "org/foo/bar/2.0/bar-2.0.jar";

        writeWithContent( TEST_FS, path2, content );
        writeWithContent( TEST_FS, path2 + ext, content );

        CassandraPathDBTool pathDBTool = new CassandraPathDBTool( pathDB.getSession(), KEYSPACE, TEST_FS );

        List<String> all = new ArrayList<>();
        pathDBTool.traverse( "/", "all", s -> {
            //System.out.println( s );
            all.add( s );
        } );

        /* All should contains:
        /
        /org
        /org/foo
        /org/foo/bar
        /org/foo/bar/1.0
        /org/foo/bar/1.0/bar-1.0.jar
        /org/foo/bar/1.0/bar-1.0.jar.http-metadata.json
        /org/foo/bar/2.0
        /org/foo/bar/2.0/bar-2.0.jar
        /org/foo/bar/2.0/bar-2.0.jar.http-metadata.json
        */

        String absolutePath1 = "/" + path1;
        assertThat( "Path missing", all.contains( absolutePath1 ), equalTo( true ) );

        pathDBTool.delete( absolutePath1 );

        pathDBTool.get( absolutePath1, dtxPathMap -> {
            assertNull( dtxPathMap );
        } );

        List<String> allAfterDelete = new ArrayList<>();
        pathDBTool.traverse( "/", "all", s -> {
            //System.out.println( s );
            allAfterDelete.add( s );
        } );

        assertThat( "Path not deleted", allAfterDelete.contains( absolutePath1 ), equalTo( false ) );
    }

}
