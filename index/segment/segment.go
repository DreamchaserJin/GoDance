/**
 * @Author hz
 * @Date 6:11 AM$ 5/21/22$
 * @Note 段相关的类型和方法
 **/

package segment

import (
	"encoding/json"
	"errors"
	"fmt"
	"gdindex/tree"
	"os"
	"utils"
)

type Segment struct {
	StartDocId  uint32            `json:"startDocId"`
	MaxDocId    uint32            `json:"maxDocId"`
	SegmentName string            `json:"segmentName"`
	FieldInfos  map[string]uint32 `json:"fields"`
	Logger      *utils.Log4FE     `json:"-"`

	fields   map[string]*Field
	isMemory bool
	btdb     *tree.BTreeDB
}

// NewEmptySegmentByFieldsInfo
// @Description 根据字段信息创建段
// @Param segmentName  段名
// @Param start  文档起始Id
// @Param fields  字段信息
// @Return 新建的段
func NewEmptySegmentByFieldsInfo(segmentName string, start uint32, fields map[string]uint32, logger *utils.Log4FE) *Segment {
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
		FieldInfos:  make(map[string]uint32),
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
	if _, ok := seg.FieldInfos[fieldName]; ok {
		seg.Logger.Warn("[WARN] Segment has field [%v]", fieldName)
		return errors.New("segment has field")
	}
	if seg.isMemory && !seg.IsEmpty() {
		seg.Logger.Warn("[WARN] Segment has field [%v]", fieldName)
		return errors.New("segment can't add field")
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
func (seg *Segment) AddDocument(docId uint32, content map[string]string) error {
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
// @Description 根据 docId 获取文档内容（未完成）
// @Param docId 文档ID
// @Return map[string]string 返回的内容
// @Return bool 是否找到文档
func (seg *Segment) GetDocument(docId uint32) (map[string]string, bool) {
	return nil, false
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
func (seg *Segment) MergeSegments(sgs []*Segment, delDocSet map[uint32]struct{}) error {
	seg.Logger.Info("[INFO] MergeSegments [%v] Start", seg.SegmentName)

	btdbName := fmt.Sprintf("%v%v", seg.SegmentName, "seg.db")
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
