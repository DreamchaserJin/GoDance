/**
 * @Author hz
 * @Date 6:03 AM 5/21/22
 * @Note
 **/

package gdindex

import (
	"GoDance/index/segment"
	"GoDance/index/tree"
	"GoDance/utils"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
)

// Index 索引类
type Index struct {
	Name              string            `json:"name"`
	PathName          string            `json:"pathName"`
	Fields            map[string]uint64 `json:"fields"`
	PrimaryKey        string            `json:"primaryKey"`
	StartDocId        uint64            `json:"startDocId"`
	MaxDocId          uint64            `json:"maxDocId"`
	DelDocNum         int               `json:"delDocNum"`
	NextSegmentSuffix uint64            `json:"nextSegmentSuffix"`
	SegmentNames      []string          `json:"segmentNames"`

	segments      []*segment.Segment
	memorySegment *segment.Segment
	primary       *tree.BTreeDB
	bitmap        *utils.Bitmap

	pkMap map[int64]string // 内存中的主键信息

	segmentMutex *sync.Mutex
	Logger       *utils.Log4FE `json:"-"`
}

// NewEmptyIndex
// @Description 创建新索引
// @Param name
// @Param pathname
// @Return
func NewEmptyIndex(name, pathname string, logger *utils.Log4FE) *Index {
	idx := &Index{
		Name:              name,
		PathName:          pathname,
		Fields:            make(map[string]uint64),
		PrimaryKey:        "",
		StartDocId:        0,
		MaxDocId:          0,
		NextSegmentSuffix: 1000,
		SegmentNames:      make([]string, 0),
		segments:          make([]*segment.Segment, 0),
		pkMap:             make(map[int64]string),
		segmentMutex:      new(sync.Mutex),
		Logger:            logger,
	}

	bitmapName := fmt.Sprintf("%v%v.bitmap", pathname, name)
	utils.MakeBitmapFile(bitmapName)
	idx.bitmap = utils.NewBitmap(bitmapName)

	delFileName := fmt.Sprintf("%v%v.del", pathname, name)
	delFile, err := os.Create(delFileName)
	defer delFile.Close()
	if err != nil {
		logger.Error("[ERROR] Create delFile ERROR : %v", err)
		return idx
	}

	return idx
}

// NewIndexFromLocalFile
// @Description 反序列化索引
// @Param name 索引名
// @Param pathname 索引的存储路径
// @Return 返回索引
func NewIndexFromLocalFile(name, pathname string, logger *utils.Log4FE) *Index {

	idx := &Index{
		Name:         name,
		PathName:     pathname,
		Fields:       make(map[string]uint64),
		SegmentNames: make([]string, 0),
		segments:     make([]*segment.Segment, 0),
		pkMap:        make(map[int64]string),
		segmentMutex: new(sync.Mutex),
		Logger:       logger,
	}

	metaFileName := fmt.Sprintf("%v%v.meta", pathname, name)
	buffer, err := utils.ReadFromJson(metaFileName)
	if err != nil {
		return idx
	}

	err = json.Unmarshal(buffer, &idx)
	if err != nil {
		return idx
	}

	for _, segmentName := range idx.SegmentNames {
		seg := segment.NewSegmentFromLocalFile(segmentName, logger)
		idx.segments = append(idx.segments, seg)
	}

	segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)

	fields := make(map[string]uint64)

	for fieldName, fieldType := range idx.Fields {
		if fieldType != utils.IDX_TYPE_PK {
			fields[fieldName] = fieldType
		}
	}

	// fmt.Println(fields)

	idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
	idx.NextSegmentSuffix++

	bitmapName := fmt.Sprintf("%v%v.bitmap", pathname, idx.Name)
	idx.bitmap = utils.NewBitmap(bitmapName)

	if idx.PrimaryKey != "" {
		primaryName := fmt.Sprintf("%v%v_primary.pk", idx.PathName, idx.Name)
		idx.primary = tree.NewBTDB(primaryName, logger)
	}

	idx.Logger.Info("[INFO] Load Index %v success", idx.Name)

	return idx
}

// AddField
// @Description 索引新增字段
// @Param field  新增的字段描述信息
// @Return 任何error
func (idx *Index) AddField(field segment.SimpleFieldInfo) error {
	if _, ok := idx.Fields[field.FieldName]; ok {
		idx.Logger.Info("[INFO] Load Index %v success", idx.Name)
		return nil
	}

	idx.Fields[field.FieldName] = field.FieldType

	// 如果是主键 则替换当前主键，只要有文档内容就不应该替换主键
	if field.FieldType == utils.IDX_TYPE_PK {
		idx.PrimaryKey = field.FieldName
		primaryBtree := fmt.Sprintf("%v%v_primary.pk", idx.PathName, idx.Name)
		idx.primary = tree.NewBTDB(primaryBtree, idx.Logger)
		idx.primary.AddBTree(field.FieldName)
	} else {
		idx.segmentMutex.Lock()
		defer idx.segmentMutex.Unlock()

		if idx.memorySegment == nil {
			// 如果内存段为 nil 则新建一个内存段并添加字段
			segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
			fields := make(map[string]uint64)
			for fieldName, fieldType := range idx.Fields {
				if fieldType != utils.IDX_TYPE_PK {
					fields[fieldName] = fieldType
				}
			}
			idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
			idx.NextSegmentSuffix++
		} else if idx.memorySegment.IsEmpty() {
			// 如果内存段大小为0，则直接添加字段
			err := idx.memorySegment.AddField(field)
			if err != nil {
				idx.Logger.Error("[ERROR] Add Field Error : %v", err)
				return err
			}
		} else {
			// 如果内存段不为空，则序列化内存段，重新创建一个内存段，这个新的内存段有新增的属性
			tempSegment := idx.memorySegment

			if err := tempSegment.Serialization(); err != nil {
				return err
			}
			idx.segments = append(idx.segments, tempSegment)
			idx.SegmentNames = append(idx.SegmentNames, tempSegment.SegmentName)

			segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
			fields := make(map[string]uint64)
			for fieldName, fieldType := range idx.Fields {
				if fieldType != utils.IDX_TYPE_PK {
					fields[fieldName] = fieldType
				}
			}
			idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
			idx.NextSegmentSuffix++
		}
	}
	return idx.storeIndex()
}

// DeleteField
// @Description: 删除索引中的某个字段
// @Param fieldName 要删除的字段名
// @Return error 任何错误
func (idx *Index) DeleteField(fieldName string) error {
	if _, ok := idx.Fields[fieldName]; !ok {
		idx.Logger.Warn("[WARN] Field Not Found : %v", fieldName)
		return nil
	}

	if fieldName == idx.PrimaryKey {
		idx.Logger.Warn("[WARN] PrimaryKey Can't Delete : %v", fieldName)
		return nil
	}

	idx.segmentMutex.Lock()
	defer idx.segmentMutex.Unlock()

	delete(idx.Fields, fieldName)

	if idx.memorySegment == nil {
		segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
		fields := make(map[string]uint64)
		for fieldName, fieldType := range idx.Fields {
			if fieldType != utils.IDX_TYPE_PK {
				fields[fieldName] = fieldType
			}
		}
		idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
		idx.NextSegmentSuffix++
	} else if idx.memorySegment.IsEmpty() {
		err := idx.memorySegment.DeleteField(fieldName)
		if err != nil {
			idx.Logger.Error("[ERROR] Delete Field Error : %v", err)
			return err
		}
	} else {
		tempSegment := idx.memorySegment

		if err := tempSegment.Serialization(); err != nil {
			return err
		}
		idx.segments = append(idx.segments, tempSegment)
		idx.SegmentNames = append(idx.SegmentNames, tempSegment.SegmentName)

		segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
		fields := make(map[string]uint64)
		for fieldName, fieldType := range idx.Fields {
			if fieldType != utils.IDX_TYPE_PK {
				fields[fieldName] = fieldType
			}
		}
		idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
		idx.NextSegmentSuffix++
	}

	return idx.storeIndex()
}

// AddDocument
// @Description: 新增文档
// @Param content 一个map，key是字段，value是内容
// @Return uint32 文档Id
// @Return error 任何error
func (idx *Index) AddDocument(content map[string]string) (uint64, error) {
	if len(idx.Fields) == 0 {
		idx.Logger.Error("[ERROR] Index has no Field")
		return 0, errors.New("index has no Field")
	}

	if idx.memorySegment == nil {
		idx.segmentMutex.Lock()

		segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)

		fields := make(map[string]uint64)
		for fieldName, fieldType := range idx.Fields {
			if fieldType != utils.IDX_TYPE_PK {
				fields[fieldName] = fieldType
			}
		}
		idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
		idx.NextSegmentSuffix++

		if err := idx.storeIndex(); err != nil {
			idx.segmentMutex.Unlock()
			return 0, err
		}
		idx.segmentMutex.Unlock()
	}

	docId := idx.MaxDocId
	idx.MaxDocId++

	if idx.PrimaryKey != "" {

		pkval, err := strconv.Atoi(content[idx.PrimaryKey])
		if err != nil {
			return 0, err
		}

		idx.pkMap[int64(pkval)] = fmt.Sprintf("%v", docId)

		if idx.MaxDocId%50000 == 0 {
			idx.primary.SetBatch(idx.PrimaryKey, idx.pkMap)
			idx.pkMap = nil
			idx.pkMap = make(map[int64]string)
		}

	}
	return docId, idx.memorySegment.AddDocument(docId, content)
}

// UpdateDocument
// @Description: 更新文档的内容，先删除再添加
// @Param content 更新后的内容
// @Return error  任何错误
func (idx *Index) UpdateDocument(content map[string]string) error {
	if _, ok := content[idx.PrimaryKey]; !ok {
		idx.Logger.Error("[ERROR] Primary Key Not Found %v", idx.PrimaryKey)
		return errors.New("no Primary Key")
	}

	pk, err := strconv.ParseInt(content[idx.PrimaryKey], 10, 64)
	if err != nil {
		return err
	}

	oldDocId, ok := idx.findPrimaryKey(pk)
	if idx.bitmap.GetBit(oldDocId) == 1 {
		return errors.New("doc has been deleted or not exist")
	}
	if ok {
		success := idx.bitmap.SetBit(uint64(oldDocId), 1)
		if success {
			idx.deleteDocumentByDocId(oldDocId)
		}
	}

	if err := idx.updatePrimaryKey(pk, oldDocId); err != nil {
		return err
	}

	docId := idx.MaxDocId
	idx.MaxDocId++
	return idx.memorySegment.AddDocument(docId, content)
}

// GetDocument
// @Description: 根据文档ID获取文档内容
// @Param docId 文档ID
// @Return map[string]string 文档内容，key是字段名，value是内容
func (idx *Index) GetDocument(docId uint64) (map[string]string, bool) {
	for _, seg := range idx.segments {
		if docId >= seg.StartDocId && docId < seg.MaxDocId {
			return seg.GetDocument(docId)
		}
	}
	return idx.memorySegment.GetDocument(docId)
}

// DeleteDocument
// @Description: 根据主键删除文档
// @param primaryKey 根据
// @return error 任何错误
func (idx *Index) DeleteDocument(primaryKey string) error {

	pk, err := strconv.ParseInt(primaryKey, 10, 64)

	if err != nil {
		return err
	}

	docId, ok := idx.findPrimaryKey(pk)
	if ok {
		if idx.bitmap.GetBit(docId) == 1 {
			return nil
		}
		success := idx.bitmap.SetBit(uint64(docId), 1)
		if success {
			idx.deleteDocumentByDocId(docId)
		}
		return nil
	}

	return err
}

// SyncMemorySegment
// @Description 内存段序列化
// @Return 任何error
func (idx *Index) SyncMemorySegment() error {
	if idx.memorySegment == nil {
		return nil
	}
	idx.segmentMutex.Lock()
	defer idx.segmentMutex.Unlock()

	if idx.memorySegment.IsEmpty() {
		return nil
	}

	if err := idx.memorySegment.Serialization(); err != nil {
		idx.Logger.Error("[ERROR] Segment Serialization Error : %v", err)
		return err
	}

	segmentName := idx.memorySegment.SegmentName

	idx.memorySegment.Close()
	idx.memorySegment = nil

	newSegment := segment.NewSegmentFromLocalFile(segmentName, idx.Logger)

	idx.segments = append(idx.segments, newSegment)

	return idx.storeIndex()

}

// CheckMerge
// @Description
func (idx *Index) CheckMerge() {

}

// MergeSegments
// @Description 合并段
// @Param start  合并段的起点
// @Return 任何error
func (idx *Index) MergeSegments(start int) error {
	startIdx := -1

	idx.segmentMutex.Lock()
	defer idx.segmentMutex.Unlock()

	if len(idx.segments) == 1 {
		return nil
	}

	// start 小于 0 ，从头开始检索
	if start < 0 {
		for i := range idx.segments {
			if idx.segments[i].MaxDocId-idx.segments[i].StartDocId < 1000000 {
				startIdx = i
				break
			}
		}
	} else {
		if start > len(idx.segments)-1 {
			return nil
		}
		startIdx = start
	}

	if startIdx == -1 {
		return nil
	}

	needMergeSegments := idx.segments[startIdx:]

	segmentName := fmt.Sprintf("%v%v_%v/", idx.PathName, idx.Name, idx.NextSegmentSuffix)
	err := os.MkdirAll(segmentName, 0755)

	if err != nil {
		idx.Logger.Error("Mkdir error : %v", err)
	}
	fields := make(map[string]uint64)
	for fieldName, fieldType := range idx.Fields {
		if fieldType != utils.IDX_TYPE_PK {
			fields[fieldName] = fieldType
		}
	}

	tmpSegment := segment.NewEmptySegmentByFieldsInfo(segmentName, needMergeSegments[0].StartDocId, fields, idx.Logger)
	idx.NextSegmentSuffix++

	if err := idx.storeIndex(); err != nil {
		return err
	}
	delFileName := fmt.Sprintf("%v%v.del", idx.PathName, idx.Name)

	delMmap, err := utils.NewMmap(delFileName, utils.MODE_APPEND)
	if err != nil {
		return err
	}

	delDocSet := delMmap.ReadIdsSet(0, idx.DelDocNum)

	tmpSegment.MergeSegments(needMergeSegments, delDocSet)

	tmpSegment.Close()
	tmpSegment = nil

	for _, seg := range needMergeSegments {
		seg.Destroy()
	}

	tmpSegment = segment.NewSegmentFromLocalFile(segmentName, idx.Logger)

	if startIdx > 0 {
		idx.segments = idx.segments[:startIdx]
		idx.SegmentNames = idx.SegmentNames[:startIdx]
	} else {
		idx.segments = make([]*segment.Segment, 0)
		idx.SegmentNames = make([]string, 0)
	}

	idx.segments = append(idx.segments, tmpSegment)
	idx.SegmentNames = append(idx.SegmentNames, segmentName)

	delMmap.Unmap()
	os.Truncate(delFileName, 0)
	idx.DelDocNum = 0

	return idx.storeIndex()
}

// Close
// @Description 关闭索引，从内存中回收
// @Return 任何error
func (idx *Index) Close() error {
	idx.segmentMutex.Lock()
	defer idx.segmentMutex.Unlock()

	idx.Logger.Info("[INFO] Close Index [%v]", idx.Name)

	if idx.memorySegment != nil {
		err := idx.memorySegment.Close()
		if err != nil {
			return err
		}
	}

	if idx.primary != nil {
		err := idx.primary.Close()
		if err != nil {
			return err
		}
	}

	if idx.bitmap != nil {
		err := idx.bitmap.Close()
		if err != nil {
			return err
		}
	}

	idx.Logger.Info("[INFO] Close Index [%v] Finish", idx.Name)

	return nil

}

// SearchKeyDocIds
// @Description 搜索某个字段的某个关键词的文档的方法
// @Param query 查询结构体
// @Return []utils.DocIdNode 查找到的ID
// @Return bool 是否查找成功
func (idx *Index) SearchKeyDocIds(query utils.SearchQuery) ([]utils.DocIdNode, bool) {

	// 最终返回的结果
	docIds := make([]utils.DocIdNode, 0)
	for _, seg := range idx.segments {
		docIds, _ = seg.SearchDocIds(query, idx.bitmap, docIds)
	}
	if len(docIds) > 0 {
		return docIds, true
	}
	return docIds, false
}

// SearchFilterDocIds
// @Description 针对某个过滤条件进行一次查询
// @Param query 过滤条件结构体
// @Return []utils.DocIdNode 查找到的ID
// @Return bool 是否查找成功
func (idx *Index) SearchFilterDocIds(filter utils.SearchFilters) ([]uint64, bool) {

	// 最终返回的结果
	docIds := make([]uint64, 0)
	for _, seg := range idx.segments {
		docIds, _ = seg.SearchDocFilter(filter, idx.bitmap, docIds)
	}
	if len(docIds) > 0 {
		sort.Slice(docIds, func(i, j int) bool {
			return docIds[i] < docIds[j]
		})
		return docIds, true
	}

	return docIds, false
}

// 内部方法
func (idx *Index) storeIndex() error {
	metaFileName := fmt.Sprintf("%v%v.meta", idx.PathName, idx.Name)

	if err := utils.WriteToJson(idx, metaFileName); err != nil {
		return err
	}
	if idx.PrimaryKey != "" {
		idx.primary.SetBatch(idx.PrimaryKey, idx.pkMap)
	}

	idx.pkMap = nil
	idx.pkMap = make(map[int64]string)

	return nil
}

func (idx *Index) findPrimaryKey(primaryKey int64) (uint64, bool) {
	ok, docId := idx.primary.Search(idx.PrimaryKey, primaryKey)
	if !ok {
		return 0, false
	}
	return docId, true
}

func (idx *Index) updatePrimaryKey(key int64, docId uint64) error {
	err := idx.primary.Set(idx.PrimaryKey, key, uint64(docId))

	if err != nil {
		idx.Logger.Error("[ERROR] update Put key error : %v", err)
		return err
	}
	return nil
}

func (idx *Index) deleteDocumentByDocId(docId uint64) {
	idx.DelDocNum++
	buf := make([]byte, 4)

	binary.LittleEndian.PutUint64(buf, docId)
	delFileName := fmt.Sprintf("%v%v.del", idx.PathName, idx.Name)
	delFile, err := os.OpenFile(delFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		idx.Logger.Error("[ERROR] Open DelFile Error : %v", err)
	}

	_, err = delFile.Write(buf)
	if err != nil {
		idx.Logger.Error("[ERROR] Write DelFile Error : %v", err)
	}
}
