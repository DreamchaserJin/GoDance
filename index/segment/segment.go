/**
 * @Author hz
 * @Date 6:11 AM$ 5/21/22$
 * @Note 段相关的类型和方法
 **/

package segment

import (
	"GoDance/index/tree"
	"GoDance/utils"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type Segment struct {
	StartDocId  uint64            `json:"startDocId"`  // 段内docId的最小值
	MaxDocId    uint64            `json:"maxDocId"`    // 段内docId的最大值
	SegmentName string            `json:"segmentName"` // 段的名称，序列化时文件名的一部分
	FieldInfos  map[string]uint64 `json:"fields"`      // 记录段内字段的类型信息
	Logger      *utils.Log4FE     `json:"-"`
	fields      map[string]*Field // 段内字段的
	isMemory    bool              // 标识段是否在内存中
	btdb        *tree.BTreeDB     // 段的数据库，用于存储各字段的正排索引
}

// NewEmptySegmentByFieldsInfo
// @Description 根据字段信息创建段
// @Param segmentName  段名
// @Param start  文档起始Id
// @Param fields  字段信息
// @Return 新建的段
func NewEmptySegmentByFieldsInfo(segmentName string, start uint64, fields map[string]uint64, logger *utils.Log4FE) *Segment {
	seg := &Segment{
		StartDocId:  start,
		MaxDocId:    start,
		SegmentName: segmentName,
		FieldInfos:  fields,
		Logger:      logger,
		fields:      make(map[string]*Field),
		isMemory:    true,
		btdb:        nil,
	}

	for fieldName, fieldType := range fields {
		f := newEmptyField(fieldName, start, fieldType, logger)
		seg.fields[fieldName] = f
	}

	return seg
}

// NewSegmentFromLocalFile
// @Description 反序列化段
// @Param segmentName  段名
// @Return 反序列化的段
func NewSegmentFromLocalFile(segmentName string, logger *utils.Log4FE) *Segment {

	seg := &Segment{
		StartDocId:  0,
		MaxDocId:    0,
		SegmentName: segmentName,
		FieldInfos:  make(map[string]uint64),
		Logger:      logger,
		fields:      make(map[string]*Field),
		isMemory:    false,
		btdb:        nil,
	}

	metaFileName := fmt.Sprintf("%v%v", segmentName, "seg.meta")
	buf, err := utils.ReadFromJson(metaFileName)

	if err != nil {
		return seg
	}

	err = json.Unmarshal(buf, &seg)
	if err != nil {
		return seg
	}

	btdbName := fmt.Sprintf("%v%v", segmentName, "seg.bt")
	if utils.Exist(btdbName) {
		seg.btdb = tree.NewBTDB(btdbName, logger)
	}

	for name := range seg.FieldInfos {
		nowField := newFieldFromLocalFile(name, segmentName, seg.StartDocId, seg.MaxDocId, seg.FieldInfos[name], seg.btdb, seg.Logger)
		seg.fields[name] = nowField
	}

	return seg
}

// AddField
// @Description 添加字段
// @Param newField  字段信息
// @Return 任何错误
func (seg *Segment) AddField(newField SimpleFieldInfo) error {
	if _, ok := seg.FieldInfos[newField.FieldName]; ok {
		seg.Logger.Warn("[WARN] Segment has field [%v]", newField.FieldName)
		return errors.New("segment has field")
	}
	if seg.isMemory && !seg.IsEmpty() {
		seg.Logger.Warn("[WARN] Segment has field [%v]", newField.FieldName)
		return errors.New("segment can't add field")
	}

	f := newEmptyField(newField.FieldName, seg.StartDocId, newField.FieldType, seg.Logger)
	seg.FieldInfos[newField.FieldName] = newField.FieldType
	seg.fields[newField.FieldName] = f

	return nil
}

// DeleteField
// @Description 删除字段
// @Param fieldName 字段名
// @Return error 任何error
func (seg *Segment) DeleteField(fieldName string) error {
	// 如果段中没有需要删除的字段
	if _, ok := seg.FieldInfos[fieldName]; !ok {
		seg.Logger.Warn("[WARN] Segment doesn't have field [%v]", fieldName)
		return errors.New("segment doesn't has field")
	}
	// 如果段已经存在，并且段中有数据，则段不可更改
	if seg.isMemory && !seg.IsEmpty() {
		seg.Logger.Warn("[WARN] Segment has field [%v]", fieldName)
		return errors.New("segment can't delete field")
	}

	seg.fields[fieldName].destroy()
	delete(seg.FieldInfos, fieldName)
	delete(seg.fields, fieldName)

	seg.Logger.Info("[INFO] Segment DeleteField %v", fieldName)

	return nil
}

// AddDocument
// @Description    为段里的 Field 新增文档
// @Param docId    文档ID
// @Param content  map[字段名]内容
// @Return error   任何错误
func (seg *Segment) AddDocument(docId uint64, content map[string]string) error {
	if docId != seg.MaxDocId {
		seg.Logger.Error("[ERROR] Segment AddDocument Wrong DocId[%v]  MaxDocId[%v]", docId, seg.MaxDocId)
		return errors.New("segment Maximum ID Mismatch")
	}

	// 为段里的字段添加
	for name, _ := range seg.fields {
		// 如果content里面没有字段，则添加一个空值
		if _, ok := content[name]; !ok {
			if err := seg.fields[name].addDocument(docId, ""); err != nil {
				seg.Logger.Error("[ERROR] Segment AddDocument [%v] :: %v", seg.SegmentName, err)
			}
			continue
		}

		if err := seg.fields[name].addDocument(docId, content[name]); err != nil {
			seg.Logger.Error("[ERROR] Segment AddDocument :: field[%v] value[%v] error[%v]", name, content[name], err)
		}
	}

	seg.MaxDocId++
	return nil
}

// GetDocument
// @Description 根据 docId 获取文档内容
// @Param docId 文档ID
// @Return map[string]string 返回的内容
// @Return bool 是否找到文档
func (seg *Segment) GetDocument(docId uint64) (map[string]string, bool) {

	if docId < seg.StartDocId || docId >= seg.MaxDocId {
		return nil, false
	}

	res := make(map[string]string)

	for name, field := range seg.fields {
		res[name], _ = field.getValue(docId)
	}

	return res, true

}

// SearchDocIds
// @Description 搜索段的方法
// @Param query 查询结构体
// @Param bitmap 位图，用于判断文档是否被删除
// @Param nowDocNodes 原始切片
// @Return []utils.DocIdNode 查找完成之后的切片
// @Return bool 是否查找成功
func (seg *Segment) SearchDocIds(query utils.SearchQuery,
	bitmap *utils.Bitmap, nowDocNodes []utils.DocIdNode) ([]utils.DocIdNode, bool) {

	// 倒排查询的 ID 切片
	var docIds []utils.DocIdNode
	var ok bool

	if query.Value == "" {
		return nowDocNodes, false
	} else {
		docIds, ok = seg.fields[query.FieldName].query(query.Value)
		if !ok {
			return nowDocNodes, false
		}
	}

	// bitmap去除被删除的文档
	if bitmap != nil {
		for _, docNode := range docIds {
			if bitmap.GetBit(docNode.Docid) == 0 {
				nowDocNodes = append(nowDocNodes, docNode)
			}
		}
		return nowDocNodes, true
	}

	return nowDocNodes, true
}

// SearchDocIdsSync
// @Description 搜索段的方法
// @Param query 查询结构体
// @Param bitmap 位图，用于判断文档是否被删除
// @Param nowDocNodes 原始切片
// @Return []utils.DocIdNode 查找完成之后的切片
// @Return bool 是否查找成功
func (seg *Segment) SearchDocIdsSync(query utils.SearchQuery,
	bitmap *utils.Bitmap) {

	// 倒排查询的 ID 切片
	var docIds []utils.DocIdNode
	returnDocIds := make([]utils.DocIdNode, 0)
	var ok bool

	if query.Value == "" {
		utils.DocIdChan <- make([]utils.DocIdNode, 0)
		return
	} else {
		docIds, ok = seg.fields[query.FieldName].query(query.Value)
		if !ok {
			utils.DocIdChan <- make([]utils.DocIdNode, 0)
			return
		}
	}

	// bitmap去除被删除的文档
	if bitmap != nil {
		for _, docNode := range docIds {
			if bitmap.GetBit(docNode.Docid) == 0 {
				returnDocIds = append(returnDocIds, docNode)
			}
		}
		utils.DocIdChan <- returnDocIds
		return
	}

	utils.DocIdChan <- returnDocIds
	return
}

func (seg *Segment) SearchDocFilter(filter utils.SearchFilters, bitmap *utils.Bitmap, nowDocIds []uint64) ([]uint64, bool) {

	// 倒排查询的 ID 切片
	var docIds []uint64

	if _, ok := seg.fields[filter.FieldName]; !ok {
		return nowDocIds, false
	}

	docIds, ok := seg.fields[filter.FieldName].queryFilter(filter)
	if !ok {
		return nowDocIds, false
	}

	// bitmap去除被删除的文档
	if bitmap != nil {
		for _, docId := range docIds {
			if bitmap.GetBit(docId) == 0 {
				nowDocIds = append(nowDocIds, docId)
			}
		}
		return nowDocIds, true
	}

	return nowDocIds, true

}

// Serialization
// @Description 序列化段
// @Return 任何error
func (seg *Segment) Serialization() error {
	os.MkdirAll(fmt.Sprintf(seg.SegmentName), 0755)

	btdbName := fmt.Sprintf("%v%v", seg.SegmentName, "seg.bt")
	if seg.btdb == nil {
		seg.btdb = tree.NewBTDB(btdbName, seg.Logger)
	}
	seg.Logger.Debug("[INFO] Serialization Segment : [%v] start", seg.SegmentName)

	for fieldName := range seg.FieldInfos {
		if err := seg.fields[fieldName].serialization(seg.SegmentName, seg.btdb); err != nil {
			seg.Logger.Error("[Error] Segment Serialization Error : %v", err)
			return err
		}
	}

	if err := seg.storeSegment(); err != nil {
		return err
	}

	seg.isMemory = false
	seg.Logger.Info("[INFO] Serialization Segment %v Finish", seg.SegmentName)

	return nil
}

// Close
// @Description 将段从内存中回收
// @Return 任何error
func (seg *Segment) Close() error {
	for _, field := range seg.fields {
		field.destroy()
	}

	if seg.btdb != nil {
		err := seg.btdb.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// Destroy
// @Description 将段从磁盘中移除
// @Return 任何error
func (seg *Segment) Destroy() error {
	for _, field := range seg.fields {
		field.destroy()
	}

	if seg.btdb != nil {
		seg.btdb.Close()
	}

	dirName := fmt.Sprintf("%v", seg.SegmentName)
	// fmt.Println(dirName)

	err := os.RemoveAll(dirName)
	if err != nil {
		return err
	}
	return nil
}

// IsEmpty
// @Description 判断是否是空段
// @Return 如果是空段就返回 true
func (seg *Segment) IsEmpty() bool {
	return seg.StartDocId == seg.MaxDocId
}

// MergeSegments
// @Description 合并段
// @Param sgs  需要合并的段
// @Return 任何error
func (seg *Segment) MergeSegments(sgs []*Segment, delDocSet map[uint64]struct{}) error {
	seg.Logger.Info("[INFO] MergeSegments [%v] Start", seg.SegmentName)

	btdbName := fmt.Sprintf("%v%v", seg.SegmentName, "seg.bt")
	if seg.btdb == nil {
		seg.btdb = tree.NewBTDB(btdbName, seg.Logger)
	}

	for name, _ := range seg.FieldInfos {
		allFields := make([]*Field, 0)
		for _, sg := range sgs {
			if _, ok := sg.fields[name]; !ok {
				tmpField := newEmptyFakeField(name, sg.StartDocId, sg.MaxDocId, seg.FieldInfos[name], seg.Logger)
				allFields = append(allFields, tmpField)
				continue
			}
			allFields = append(allFields, sg.fields[name])
		}
		seg.fields[name].mergeField(allFields, seg.SegmentName, seg.btdb, delDocSet)
	}

	seg.isMemory = false
	seg.MaxDocId = sgs[len(sgs)-1].MaxDocId

	return seg.storeSegment()
}

// 内部方法
func (seg *Segment) storeSegment() error {
	metaFileName := fmt.Sprintf("%v%v.meta", seg.SegmentName, "seg")
	if err := utils.WriteToJson(seg, metaFileName); err != nil {
		return err
	}
	return nil
}
