package org.commonjava.storage.pathmapped.spi;

public class FileInfo
{
    private String fileId;

    private String fileStorage;

    public String getFileId()
    {
        return fileId;
    }

    public void setFileId( String fileId )
    {
        this.fileId = fileId;
    }

    public String getFileStorage()
    {
        return fileStorage;
    }

    public void setFileStorage( String fileStorage )
    {
        this.fileStorage = fileStorage;
    }

    @Override
    public String toString()
    {
        return "FileInfo{" + "fileId='" + fileId + '\'' + ", fileStorage='" + fileStorage + '\'' + '}';
    }
}
