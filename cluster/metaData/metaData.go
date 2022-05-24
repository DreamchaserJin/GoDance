package metaData

//MetaData 用于持久化的数据
type MetaData struct {
	//clusterUUID：集群的唯一id
	clusterUUID string
	//当前版本号，每次更新加1
	version int64
	//持久化的集群设置
	setting map[string]string
	//所有Index的Meta
	indices map[string]IndexMetaData
}
