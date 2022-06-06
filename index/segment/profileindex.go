/**
 * @Author hz
 * @Date 6:08 AM$ 5/28/22$
 * @Note B+树 正排索引，用于 数值类型和日期时间类型
 **/

package segment

import (
	"GoDance/index/tree"
	"GoDance/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
)

type profileindex struct {
	curDocId      uint64
	isMemory      bool
	fieldType     uint64
	fieldName     string
	pfiMmap       *utils.Mmap
	memoryHashMap map[int64][]uint64
	Logger        *utils.Log4FE
	btree         *tree.BTreeDB
}

// newEmptyProfileIndex
// @Description 新建一个空的正排索引
// @Param fieldType 字段类型
// @Param startDocId 起始文档ID
// @Param fieldName 字段名
// @Param logger  日志
// @Return *profileindex 正排索引的引用
func newEmptyProfileIndex(fieldType, startDocId uint64, fieldName string, logger *utils.Log4FE) *profileindex {
	pfi := &profileindex{
		curDocId:      startDocId,
		isMemory:      true,
		fieldType:     fieldType,
		fieldName:     fieldName,
		memoryHashMap: make(map[int64][]uint64),
		Logger:        logger,
	}
	return pfi
}

func newProfileIndexFromLocalFile(btdb *tree.BTreeDB, fieldType uint64, fieldName, segmentName string,
	pfiMmap *utils.Mmap, logger *utils.Log4FE) *profileindex {

	pfi := &profileindex{
		isMemory:  false,
		fieldType: fieldType,
		fieldName: fieldName,
		pfiMmap:   pfiMmap,
		Logger:    logger,
		btree:     btdb,
	}
	return pfi
}

// addDocument
// @Description 正排索引新增文档
// @Param docId 文档ID
// @Param contentStr 内容
// @Return error 任何错误
func (pfi *profileindex) addDocument(docId uint64, contentStr string) error {
	if docId != pfi.curDocId {
		return errors.New("profileindex AddDocument :: Wrong DocId Number")
	}
	pfi.Logger.Trace("[TRACE] profileindex AddDocument :: docid %v content %v", docId, contentStr)

	var value int64 = -1

	switch pfi.fieldType {
	case utils.IDX_TYPE_NUMBER:

		intValue, err := strconv.ParseInt(contentStr, 10, 64)
		if err != nil {
			intValue = -1
		}
		value = intValue
	case utils.IDX_TYPE_FLOAT:
		floatValue, err := strconv.ParseFloat(contentStr, 64)
		if err != nil {
			floatValue = -100
		}
		value = int64(floatValue * 100)
	case utils.IDX_TYPE_DATE:
		value, _ = utils.IsDateTime(contentStr)
	}

	if _, ok := pfi.memoryHashMap[value]; !ok {
		var docIds []uint64
		docIds = append(docIds, docId)
		pfi.memoryHashMap[value] = docIds
	} else {
		pfi.memoryHashMap[value] = append(pfi.memoryHashMap[value], docId)
	}

	pfi.curDocId++
	return nil
}

func (pfi *profileindex) serialization(segmentName string, btdb *tree.BTreeDB) error {
	idxFileName := fmt.Sprintf("%v%v_profileindex.pfi", segmentName, pfi.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	pfi.btree = btdb

	if err != nil {
		return err
	}
	defer idxFd.Close()

	leafNodes := make(map[int64]string)
	nowOffset := uint64(0)

	for key, value := range pfi.memoryHashMap {

		lens := len(value)

		lenBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(lens))
		idxFd.Write(lenBuffer)

		stringBuffer := new(bytes.Buffer)

		err = binary.Write(stringBuffer, binary.LittleEndian, value)
		if err != nil {
			pfi.Logger.Error("[Error] invert Serialization Error : %v", err)
			return err
		}

		idxFd.Write(stringBuffer.Bytes())
		leafNodes[key] = fmt.Sprintf("%v", nowOffset)

		nowOffset += uint64(lens)*8 + 8
	}

	err = pfi.btree.SetBatch(pfi.fieldName, leafNodes)
	if err != nil {
		return err
	}

	pfi.memoryHashMap = nil
	pfi.isMemory = false
	pfi.Logger.Trace("[Trace] invert Serialization Finish, Writing to : %v%v_profileindex.pfi", segmentName, pfi.fieldName)
	pfi.Logger.Trace("[Trace] invert Serialization Finish, Writing to : %v.db", segmentName)

	return nil

}

func (pfi *profileindex) destroy() {
	pfi.memoryHashMap = nil
}

func (pfi *profileindex) setPfiMmap(mmap *utils.Mmap) {
	pfi.pfiMmap = mmap
}

func (pfi *profileindex) setBtree(btdb *tree.BTreeDB) {
	pfi.btree = btdb
}

func (pfi *profileindex) mergeProfileIndex(inverts []*profileindex, segmentName string, btdb *tree.BTreeDB) error {
	idxFileName := fmt.Sprintf("%v%v_profileindex.pfi", segmentName, pfi.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer idxFd.Close()

	fi, _ := idxFd.Stat()
	totalOffset := int(fi.Size())

	pfi.btree = btdb
	type pfiMerge struct {
		p      *profileindex
		key    int64
		docids []uint64
	}

	pfis := make([]pfiMerge, 0)

	for _, i := range inverts {
		if i.btree == nil {
			continue
		}

		key, _, ok := i.GetFirstKV()
		if !ok {
			continue
		}

		docIds, _ := i.queryTerm(key)
		pfis = append(pfis, pfiMerge{
			p:      i,
			key:    key,
			docids: docIds,
		})
	}

	resflag := 0
	for i := range pfis {
		resflag = resflag | (1 << uint(i))
	}
	flag := 0
	for flag != resflag {
		minKey := pfis[0].key
		meridxs := make([]int, 0)
		for idx, p := range pfis {
			if flag>>uint(idx)&1 != 0 {
				continue
			}
			if minKey > p.key {
				minKey = p.key
				meridxs = make([]int, 0)
				meridxs = append(meridxs, idx)
			} else if minKey == p.key {
				meridxs = append(meridxs, idx)
				continue
			}
		}

		value := make([]uint64, 0)

		for _, idx := range meridxs {
			value = append(value, pfis[idx].docids...)

			key, _, ok := pfis[idx].p.GetNextKV(pfis[idx].key)
			if !ok {
				flag = flag | (1 << uint(idx))
				continue
			}

			pfis[idx].key = key
			pfis[idx].docids, ok = pfis[idx].p.queryTerm(key)
		}

		lens := len(value)
		lenBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(lens))
		idxFd.Write(lenBuffer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			pfi.Logger.Error("[ERROR] invert --> Merge :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())
		pfi.btree.Set(pfi.fieldName, minKey, uint64(totalOffset))
		totalOffset = totalOffset + 8 + lens*8
	}

	pfi.memoryHashMap = nil
	pfi.isMemory = false

	return nil
}

func (pfi *profileindex) GetFirstKV() (int64, uint64, bool) {

	if pfi.btree == nil {
		return 0, 0, false
	}
	return pfi.btree.GetFirstKV(pfi.fieldName)
}

func (pfi *profileindex) GetNextKV(key int64) (int64, uint64, bool) {

	if pfi.btree == nil {
		return 0, 0, false
	}

	return pfi.btree.GetNextKV(pfi.fieldName, key)
}

func (pfi *profileindex) queryTerm(key int64) ([]uint64, bool) {

	if pfi.isMemory == true {
		docIds, ok := pfi.memoryHashMap[key]
		if ok {
			return docIds, true
		}
	} else if pfi.pfiMmap != nil {
		ok, offset := pfi.btree.Search(pfi.fieldName, key)
		if !ok {
			return nil, false
		}
		lens := pfi.pfiMmap.ReadInt64(int64(offset))

		res := pfi.pfiMmap.ReadIdsArray(uint64(offset)+8, int(lens))
		return res, true
	}

	return nil, false
}

func (pfi *profileindex) queryRange(keyMin, keyMax int64) ([]uint64, bool) {

	res := make([]uint64, 0)
	if pfi.isMemory == true {
		for k, v := range pfi.memoryHashMap {
			if k >= keyMin && k <= keyMax {
				res = append(res, v...)
			}
		}
		return res, true
	} else if pfi.pfiMmap != nil {
		ok, offsets := pfi.btree.SearchRange(pfi.fieldName, keyMin, keyMax)
		if ok {
			for _, offset := range offsets {
				lens := pfi.pfiMmap.ReadInt64(int64(offset))
				IdsArray := pfi.pfiMmap.ReadIdsArray(uint64(offset)+8, int(lens))
				res = append(res, IdsArray...)
			}
			return res, true
		}
	}
	return nil, false
}
