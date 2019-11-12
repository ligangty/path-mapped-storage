package org.commonjava.storage.pathmapped.model;

public interface FileChecksum
{
    String getFileId();

    String getChecksum();

    String getStorage();
}
