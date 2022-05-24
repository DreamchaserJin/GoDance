/**
 * @Author hz
 * @Date 6:03 AM 5/21/22
 * @Note
 **/

package gdindex

import (
	"encoding/json"
	"fmt"
	"gdindex/segment"
	"gdindex/tree"
	"os"
	"sync"
	"time"
	"utils"
)

// Index 索引类
type Index struct {
	Name              string            `json:"name"`
	PathName          string            `json:"pathName"`
	Fields            map[string]uint32 `json:"fields"`
	PrimaryKey        string            `json:"primaryKey"`
	StartDocId        uint32            `json:"startDocId"`
	MaxDocId          uint32            `json:"maxDocId"`
	NextSegmentSuffix uint64            `json:"nextSegmentSuffix"`
	SegmentNames      []string          `json:"segmentNames"`

	segments      []*segment.Segment
	memorySegment *segment.Segment
	primary       *tree.BTreeDB
	bitmap        *utils.Bitmap

	pkMap map[string]string // 内存中的主键信息

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
		Fields:            make(map[string]uint32),
		PrimaryKey:        "",
		StartDocId:        0,
		MaxDocId:          0,
		NextSegmentSuffix: 1000,
		SegmentNames:      make([]string, 0),
		segments:          make([]*segment.Segment, 0),
		pkMap:             make(map[string]string),
		segmentMutex:      new(sync.Mutex),
	}

	bitmapName := fmt.Sprintf("%v%v.bitmap", pathname, name)
	utils.MakeBitmapFile(bitmapName)
	idx.bitmap = utils.NewBitmap(bitmapName)

	return idx
}

// NewIndexFromLocalFile
// @Description 反序列化索引
// @Param name 索引名
// @Param pathname 索引的存储路径
// @Return 返回索引
func NewIndexFromLocalFile(name, pathname string, logger *utils.Log4FE) *Index {

	idx := &Index{
		Name:              name,
		PathName:          pathname,
		Fields:            make(map[string]uint32),
		StartDocId:        0,
		MaxDocId:          0,
		NextSegmentSuffix: 1000,
		SegmentNames:      make([]string, 0),
		segments:          make([]*segment.Segment, 0),
		pkMap:             make(map[string]string),
		segmentMutex:      new(sync.Mutex),
		Logger:            logger,
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

	fields := make(map[string]uint32)

	for fieldName, fieldType := range idx.Fields {
		if fieldType != utils.IDX_TYPE_PK {
			fields[fieldName] = fieldType
		}
	}

	fmt.Println(fields)

	idx.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentName, idx.MaxDocId, fields, idx.Logger)
	idx.NextSegmentSuffix++

	bitmapName := fmt.Sprintf("%v%v.bitmat", pathname, idx.Name)
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
//func (this *Index) AddField(field segment.SimpleFieldInfo) error {
//
//	if _, ok := this.Fields[field.FieldName]; ok {
//		this.Logger.Warn("[WARN] field %v Exist ", field.FieldName)
//		return nil
//	}
//
//	this.Fields[field.FieldName] = field.FieldType
//
//	if field.FieldType == utils.IDX_TYPE_PK {
//		this.PrimaryKey = field.FieldName
//		primaryname := fmt.Sprintf("%v%v_primary.pk", this.PathName, this.Name)
//		this.primary = tree.NewBTDB(primaryname, this.Logger)
//		this.primary.AddTree(field.FieldName)
//	} else {
//		this.segmentMutex.Lock()
//		defer this.segmentMutex.Unlock()
//
//		if this.memorySegment == nil {
//			segmentname := fmt.Sprintf("%v%v_%v", this.PathName, this.Name, this.NextSegmentSuffix)
//			fields := make(map[string]uint32)
//			for fieldName, fieldType := range this.Fields {
//				if fieldType != utils.IDX_TYPE_PK {
//					fields[fieldName] = fieldType
//				}
//
//			}
//			this.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentname, this.MaxDocId, fields, this.Logger)
//			this.NextSegmentSuffix++
//
//		} else if this.memorySegment.IsEmpty() {
//			err := this.memorySegment.AddField(field)
//			if err != nil {
//				this.Logger.Error("[ERROR] Add Field Error  %v", err)
//				return err
//			}
//		} else {
//			tmpsegment := this.memorySegment
//			if err := tmpsegment.Serialization(); err != nil {
//				return err
//			}
//			this.segments = append(this.segments, tmpsegment)
//			this.SegmentNames = make([]string, 0)
//			for _, seg := range this.segments {
//				this.SegmentNames = append(this.SegmentNames, seg.SegmentName)
//			}
//
//			segmentname := fmt.Sprintf("%v%v_%v", this.PathName, this.Name, this.NextSegmentSuffix)
//			fields := make(map[string]uint32)
//			for fieldName, fieldType := range this.Fields {
//				if fieldType != utils.IDX_TYPE_PK {
//					fields[fieldName] = fieldType
//				}
//
//			}
//			this.memorySegment = segment.NewEmptySegmentByFieldsInfo(segmentname, this.MaxDocId, fields, this.Logger)
//			this.NextSegmentSuffix++
//		}
//	}
//	return this.storeIndex()
//}

// AddDocument
// @Description 新增文档
// @Param content 一个map，key是字段，value是内容
// @Return 文档Id，任何error
//func (idx *Index) AddDocument(content map[string]string) (uint32, error) {
//
//}

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
// @Description 返回一些信息用于判断是否需要合并
// @Return 一些信息（未完成）
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
	fields := make(map[string]uint32)
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

	tmpSegment.MergeSegments(needMergeSegments)

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

	return idx.storeIndex()
}

// Close
// @Description 关闭索引，从内存中移除
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

// 搜索相关的方法(还没写 API)

// 内部方法
func (idx *Index) storeIndex() error {
	metaFileName := fmt.Sprintf("%v%v.meta", idx.PathName, idx.Name)

	if err := utils.WriteToJson(idx, metaFileName); err != nil {
		return err
	}
	startTime := time.Now()
	idx.Logger.Debug("[INFO] start storeIndex %v", startTime)

	// idx.primary.MultiSet(idx.PrimaryKey, idx.pkmap)

	idx.Logger.Debug("[INFO] start storeIndex %v", startTime)

	idx.pkMap = nil
	idx.pkMap = make(map[string]string)

	return nil
}
