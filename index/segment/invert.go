/**
 * @Author hz
 * @Date 6:20 AM$ 5/21/22$
 * @Note
 **/

package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"gdindex/tree"
	"os"
	"utils"
)

type invert struct {
	curDocId      uint32
	isMemory      bool
	fieldType     uint32
	fieldName     string
	idxMmap       *utils.Mmap
	memoryHashMap map[string][]utils.DocIdNode
	Logger        *utils.Log4FE
	btree         *tree.BTreeDB
}

//func newEmptyFakeInvert(fieldType uint32, startDocId uint32, fieldName string, logger *utils.Log4FE) *invert {
//	ivt := &invert{
//		curDocId:      startDocId,
//		isMemory:      true,
//		fieldType:     fieldType,
//		fieldName:     fieldName,
//		memoryHashMap: nil,
//		Logger:        logger,
//	}
//	return ivt
//}

func newEmptyInvert(fieldType uint32, startDocId uint32, fieldName string, logger *utils.Log4FE) *invert {
	ivt := &invert{
		curDocId:      startDocId,
		isMemory:      true,
		fieldType:     fieldType,
		fieldName:     fieldName,
		memoryHashMap: nil,
		Logger:        logger,
	}
	return ivt
}

func newInvertFromLocalFile(btdb *tree.BTreeDB, fieldType uint32, fieldName, segmentName string,
	idxMmap *utils.Mmap, logger *utils.Log4FE) *invert {

	ivt := &invert{
		isMemory:  false,
		fieldType: fieldType,
		fieldName: fieldName,
		idxMmap:   idxMmap,
		Logger:    logger,
		btree:     btdb,
	}

	return ivt
}

func (ivt *invert) addDocument(docId uint32, contentStr string) error {
	return nil
}

func (ivt *invert) serialization(segmentName string, btree *tree.BTreeDB) error {
	idxFileName := fmt.Sprintf("%v%v_invert.idx", segmentName, ivt.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return err
	}
	defer idxFd.Close()

	leafNodes := make(map[string]uint64)
	nowOffset := uint64(0)

	for key, value := range ivt.memoryHashMap {

		lens := len(key)

		lenBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(lens))
		idxFd.Write(lenBuffer)

		stringBuffer := new(bytes.Buffer)

		err = binary.Write(stringBuffer, binary.LittleEndian, value)
		if err != nil {
			ivt.Logger.Error("[Error] invert Serialization Error : %v", err)
			return err
		}

		idxFd.Write(stringBuffer.Bytes())
		leafNodes[key] = nowOffset

		ivt.btree.Set(ivt.fieldName, key, nowOffset)

		nowOffset += uint64(lens)*8 + 8
	}

	ivt.memoryHashMap = nil
	ivt.isMemory = false
	ivt.Logger.Trace("[Trace] invert Serialization Finish, Writing to : %v%v_invert.idx", segmentName, ivt.fieldName)
	ivt.Logger.Trace("[Trace] invert Serialization Finish, Writing to : %v.db", segmentName)

	return nil

}

func (ivt *invert) destroy() {
	ivt.memoryHashMap = nil
}

func (ivt *invert) setIdxMmap(mmap *utils.Mmap) {
	ivt.idxMmap = mmap
}

func (ivt *invert) setBtree(btdb *tree.BTreeDB) {
	ivt.btree = btdb
}

func (ivt *invert) mergeInvert(inverts []*invert, segmentName string, btdb *tree.BTreeDB) error {
	idxFileName := fmt.Sprintf("%v%v_invert.idx", segmentName, ivt.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer idxFd.Close()

	fi, _ := idxFd.Stat()
	totalOffset := int(fi.Size())

	ivt.btree = btdb
	type ivtMerge struct {
		ivt    *invert
		key    string
		docids []utils.DocIdNode
		pgnum  uint32
		index  int
	}

	ivts := make([]ivtMerge, 0)

	for _, i := range inverts {
		if i.btree == nil {
			continue
		}

		key, _, pgnum, index, ok := ivt.GetFirstKV()
		if !ok {
			continue
		}

		docIds, _ := ivt.queryTerm(key)
		ivts = append(ivts, ivtMerge{
			ivt:    i,
			key:    key,
			docids: docIds,
			pgnum:  pgnum,
			index:  index,
		})
	}

	resflag := 0
	for i := range ivts {
		resflag = resflag | (1 << uint(i))
	}
	flag := 0
	for flag != resflag {
		maxkey := ""
		meridxs := make([]int, 0)
		for idx, ivt := range ivts {

			if (flag>>uint(idx)&0x1) == 0 && maxkey < ivt.key {
				maxkey = ivt.key
				meridxs = make([]int, 0)
				meridxs = append(meridxs, idx)
				continue
			}

			if (flag>>uint(idx)&0x1) == 0 && maxkey == ivt.key {
				meridxs = append(meridxs, idx)
				continue
			}

		}

		value := make([]utils.DocIdNode, 0)

		for _, idx := range meridxs {
			value = append(value, ivts[idx].docids...)

			key, _, pgnum, index, ok := ivts[idx].ivt.GetNextKV(ivts[idx].key)
			if !ok {
				flag = flag | (1 << uint(idx))
				continue
			}

			ivts[idx].key = key
			ivts[idx].pgnum = pgnum
			ivts[idx].index = index
			ivts[idx].docids, ok = ivts[idx].ivt.queryTerm(key)

		}

		lens := len(value)
		lenBufer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBufer, uint64(lens))
		idxFd.Write(lenBufer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			ivt.Logger.Error("[ERROR] invert --> Merge :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())
		ivt.btree.Set(ivt.fieldName, maxkey, uint64(totalOffset))
		totalOffset = totalOffset + 8 + lens*utils.DOCNODE_SIZE

	}

	ivt.memoryHashMap = nil
	ivt.isMemory = false

	return nil
}

func (ivt *invert) GetFirstKV() (string, uint64, uint32, int, bool) {

	if ivt.btree == nil {
		return "", 0, 0, 0, false
	}
	return ivt.btree.GetFirstKV(ivt.fieldName)
}

func (ivt *invert) GetNextKV(key string) (string, uint64, uint32, int, bool) {

	if ivt.btree == nil {
		return "", 0, 0, 0, false
	}

	return ivt.btree.GetNextKV(ivt.fieldName, key)
}

func (ivt *invert) queryTerm(keyStr string) ([]utils.DocIdNode, bool) {

	if ivt.isMemory == true {
		docIds, ok := ivt.memoryHashMap[keyStr]
		if ok {
			return docIds, true
		}
	} else if ivt.idxMmap != nil {
		ok, offset := ivt.btree.Search(ivt.fieldName, keyStr)
		if !ok {
			return nil, false
		}
		lens := ivt.idxMmap.ReadInt64(int64(offset))

		res := ivt.idxMmap.ReadDocIdsArry(uint64(offset)+8, uint64(lens))
		return res, true
	}

	return nil, false
}
