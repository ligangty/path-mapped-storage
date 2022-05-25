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
package org.commonjava.storage.pathmapped.pathdb.datastax.util;

public class CassandraPathDBUtils
{
    public static final String PROP_CASSANDRA_HOST = "cassandra_host";

    public static final String PROP_CASSANDRA_PORT = "cassandra_port";

    public static final String PROP_CASSANDRA_USER = "cassandra_user";

    public static final String PROP_CASSANDRA_PASS = "cassandra_pass";

    public static final String PROP_CASSANDRA_KEYSPACE = "cassandra_keyspace";

    public static final String PROP_CASSANDRA_REPLICATION_FACTOR = "cassandra_replication_factor";

    public static String getSchemaCreateKeyspace( String keyspace, int replicationFactor )
    {
        return "CREATE KEYSPACE IF NOT EXISTS " + keyspace
                        + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor
                        + "};";
    }

    /*
     * 'filesystem' table provides basic file count and total size of this filesystem.
     * A counter is a 64-bit signed integer and on which only 2 operations are supported: incrementing and decrementing.
     * Table that contains a counter can only contain counters. In other words, either all the columns of a table
     * outside the PRIMARY KEY have the counter type, or none of them have it.
     */
    public static String getSchemaCreateTableFilesystem( String keyspace )
    {
        return "CREATE TABLE IF NOT EXISTS " + keyspace + ".filesystem ("
                + "filesystem varchar,"
                + "filecount counter,"
                + "size counter,"
                + "PRIMARY KEY (filesystem)"
                + ");";
    }

    public static String getSchemaCreateTablePathmap( String keyspace )
    {
        return "CREATE TABLE IF NOT EXISTS " + keyspace + ".pathmap ("
                        + "filesystem varchar,"
                        + "parentpath varchar,"
                        + "filename varchar,"
                        + "fileid varchar,"
                        + "creation timestamp,"
                        + "expiration timestamp,"
                        + "size bigint,"
                        + "filestorage varchar,"
                        + "checksum varchar,"
                        + "PRIMARY KEY ((filesystem, parentpath), filename)"
                        + ");";
    }

    public static String getSchemaCreateTableReversemap( String keyspace )
    {
        return "CREATE TABLE IF NOT EXISTS " + keyspace + ".reversemap ("
                        + "fileid varchar,"
                        + "paths set<text>,"
                        + "PRIMARY KEY (fileid)"
                        + ");";
    }

    public static String getSchemaCreateTableReclaim( String keyspace )
    {
        return "CREATE TABLE IF NOT EXISTS " + keyspace + ".reclaim ("
                        + "partition int,"
                        + "deletion timestamp,"
                        + "fileid varchar,"
                        + "checksum varchar,"
                        + "storage varchar,"
                        + "PRIMARY KEY (partition, deletion, fileid)"
                        + ");";
    }

    public static String getSchemaCreateTableFileChecksum( String keyspace )
    {
        return "CREATE TABLE IF NOT EXISTS " + keyspace + ".filechecksum ("
                        + "checksum varchar,"
                        + "fileid varchar,"
                        + "storage varchar,"
                        + "PRIMARY KEY (checksum)"
                        + ");";
    }
}
