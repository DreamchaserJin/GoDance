package engine

import (
	gdindex "GoDance/index"
	"GoDance/index/segment"
	"GoDance/utils"
	"encoding/json"
	"fmt"
	"sync"
)

type IndexInfo struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type IndexManager struct {
	indexers       map[string]*gdindex.Index
	indexMapLocker *sync.RWMutex
	IndexInfos     map[string]IndexInfo `json:"indexinfos"`
	Logger         *utils.Log4FE        `json:"-"`
}

// 一个引擎就一个索引管理器
func newIndexManager(logger *utils.Log4FE) *IndexManager {
	idm := &IndexManager{
		indexers:       make(map[string]*gdindex.Index),
		indexMapLocker: new(sync.RWMutex),
		IndexInfos:     make(map[string]IndexInfo),
		Logger:         logger,
	}

	// 如果之前有记录则进行反序列化
	if utils.Exist(fmt.Sprintf("%v%v.mgt.meta", utils.IDX_ROOT_PATH, utils.GODANCEENGINE)) {

		metaFileName := fmt.Sprintf("%v%v.mgt.meta", utils.IDX_ROOT_PATH, utils.GODANCEENGINE)
		buffer, err := utils.ReadFromJson(metaFileName)
		if err != nil {
			return idm
		}

		err = json.Unmarshal(buffer, &idm)
		if err != nil {
			return idm
		}

		for _, idxInfo := range idm.IndexInfos {
			idm.indexers[idxInfo.Name] = gdindex.NewIndexFromLocalFile(idxInfo.Name, idxInfo.Path, logger)
		}
	}

	idm.Logger.Info("[INFO]  New Index Manager ")
	return idm
}

func (idm *IndexManager) CreateIndex(indexName string, fields []segment.SimpleFieldInfo) error {

	idm.indexMapLocker.Lock()
	defer idm.indexMapLocker.Unlock()
	if _, ok := idm.indexers[indexName]; ok {
		idm.Logger.Error("[ERROR] index[%v] Exist", indexName)
		return nil
	}

	idm.indexers[indexName] = gdindex.NewEmptyIndex(indexName, utils.IDX_ROOT_PATH, idm.Logger)
	idm.IndexInfos[indexName] = IndexInfo{Name: indexName, Path: utils.IDX_ROOT_PATH}
	for _, field := range fields {
		idm.indexers[indexName].AddField(field)
	}

	return idm.storeIndexManager()
}

func (idm *IndexManager) AddField(indexName string, field segment.SimpleFieldInfo) error {

	idm.indexMapLocker.RLock()
	defer idm.indexMapLocker.RUnlock()
	if _, ok := idm.indexers[indexName]; !ok {
		idm.Logger.Error("[ERROR] index[%v] not found", indexName)
		return fmt.Errorf("[ERROR] index[%v] not found", indexName)
	}

	return idm.indexers[indexName].AddField(field)
}

func (idm *IndexManager) DeleteField(indexName string, fieldName string) error {

	idm.indexMapLocker.RLock()
	defer idm.indexMapLocker.RUnlock()
	if _, ok := idm.indexers[indexName]; !ok {
		idm.Logger.Error("[ERROR] index[%v] not found", indexName)
		return fmt.Errorf("[ERROR] index[%v] not found", indexName)
	}

	return idm.indexers[indexName].DeleteField(fieldName)
}

func (idm *IndexManager) storeIndexManager() error {
	metaFileName := fmt.Sprintf("%v%v.mgt.meta", utils.IDX_ROOT_PATH, utils.GODANCEENGINE)
	if err := utils.WriteToJson(idm, metaFileName); err != nil {
		return err
	}
	return nil
}
