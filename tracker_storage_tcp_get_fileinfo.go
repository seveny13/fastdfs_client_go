package fastdfs_client_go

// GetRemoteFileInfo  获取远程服务器的文件信息
// @remoteFileId 远程服务器的文件Id
func (c *TrackerServerTcpClient) GetRemoteFileInfo(remoteFileId string) (remoteFileInfo RemoteFileInfo, err error) {
	groupName, remoteFilename, err := splitStorageServerFileId(remoteFileId)
	if err != nil {
		return remoteFileInfo, err
	}

	storageServInfo, err := c.getStorageInfoByTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE, "", "")
	if err != nil {
		return remoteFileInfo, err
	}
	queryRemoteFile := &storageGetFileInfoHeaderBody{}
	queryRemoteFile.groupName = groupName
	queryRemoteFile.remoteFilename = remoteFilename

	if err = c.sendCmdToStorageServer(queryRemoteFile, storageServInfo); err != nil {
		return remoteFileInfo, err
	}
	remoteFileInfo.fileSize = queryRemoteFile.fileSize
	remoteFileInfo.createTimestamp = queryRemoteFile.createTimestamp
	remoteFileInfo.crc32 = queryRemoteFile.crc32
	remoteFileInfo.SourceIpAddr = queryRemoteFile.SourceIpAddr

	return remoteFileInfo, nil
}
