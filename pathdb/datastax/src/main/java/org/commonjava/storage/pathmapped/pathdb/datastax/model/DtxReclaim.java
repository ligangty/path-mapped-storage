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
package org.commonjava.storage.pathmapped.pathdb.datastax.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.commonjava.storage.pathmapped.model.Reclaim;

import java.util.Date;
import java.util.Objects;

import static org.commonjava.storage.pathmapped.pathdb.datastax.util.CassandraPathDBUtils.getHoursInDay;

@Table( name = "reclaim", readConsistency = "QUORUM", writeConsistency = "QUORUM" )
public class DtxReclaim implements Reclaim
{
    @PartitionKey
    private int partition; // partition by hours in day (0~23)

    @ClusteringColumn(0)
    private Date deletion;

    @ClusteringColumn(1)
    private String fileId;

    @Column
    private String checksum;

    @Column
    private String storage;

    public DtxReclaim()
    {
    }

    public DtxReclaim( String fileId, Date deletion, String storage, String checksum )
    {
        this.partition = getHoursInDay( deletion );
        this.fileId = fileId;
        this.deletion = deletion;
        this.storage = storage;
        this.checksum = checksum;
    }

    public int getPartition()
    {
        return partition;
    }

    public void setPartition( int partition )
    {
        this.partition = partition;
    }

    @Override
    public String getFileId()
    {
        return fileId;
    }

    public void setFileId( String fileId )
    {
        this.fileId = fileId;
    }

    @Override
    public Date getDeletion()
    {
        return deletion;
    }

    public void setDeletion( Date deletion )
    {
        this.deletion = deletion;
    }

    @Override
    public String getStorage()
    {
        return storage;
    }

    public void setStorage( String storage )
    {
        this.storage = storage;
    }

    public String getChecksum()
    {
        return checksum;
    }

    public void setChecksum( String checksum )
    {
        this.checksum = checksum;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        DtxReclaim that = (DtxReclaim) o;
        return partition == that.partition && deletion.equals( that.deletion ) && fileId.equals( that.fileId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( partition, deletion, fileId );
    }

    @Override
    public String toString()
    {
        return "DtxReclaim{" + "partition=" + partition + ", deletion=" + deletion + ", fileId='" + fileId + '\''
                        + ", checksum='" + checksum + '\'' + ", storage='" + storage + '\'' + '}';
    }
}
