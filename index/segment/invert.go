/**
 * @Author hz
 * @Date 6:20 AM$ 5/21/22$
 * @Note
 **/

package segment

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"gdindex/tree"
	"github.com/blevesearch/vellum"
	"os"
	"sort"
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
	fst           *vellum.FST
	//btree         *tree.BTreeDB
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
		fst:           nil,
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
		fst:       nil,
		//btree:     btdb,
	}
	// 从文件中读取fst文件
	fst, err := vellum.Open(fmt.Sprintf("%v%v_invert.fst", segmentName, fieldName))
	// 读取失败
	if err != nil {
		ivt.Logger.Error("[Error] file of fst read error, file name %v%v_invert.fst", segmentName, fieldName)
	}
	// 读取成功写入ivt
	ivt.fst = fst

	return ivt
}

func (ivt *invert) addDocument(docId uint32, contentStr string) error {
	return nil
}

func (ivt *invert) serialization(segmentName string, btree *tree.BTreeDB) error {
	// TODO 添加fst
	// fst存储文件名
	fstFileName := fmt.Sprintf("%v%v_invert.fst", segmentName, ivt.fieldName)
	// 打开fst文件
	fstFd, err := os.OpenFile(fstFileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	// 保证fst文件可以关闭
	defer fstFd.Close()
	// 打开idx文件，用于存储memoryHashMap, 一个倒排字典
	idxFileName := fmt.Sprintf("%v%v_invert.idx", segmentName, ivt.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		return err
	}
	defer idxFd.Close()
	// 生成fst builder用于批量写入文件
	builder, err := vellum.New(fstFd, nil)
	if err != nil {
		return err
	}
	// 保证builder正常关闭, 否则无法写入文件
	defer builder.Close()
	leafNodes := make(map[string]uint64)
	nowOffset := uint64(0)
	// 因为插入fst的key必须是有序的,所以需要记录memoryHashMap中的key值，以供排序
	keys := make([]string, len(ivt.memoryHashMap))

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

		// 不用b+树存倒排索引了
		//ivt.btree.Set(ivt.fieldName, key, nowOffset)

		nowOffset += uint64(lens)*8 + 8
	}
	// 对key进行排序
	sort.Strings(keys)
	for _, key := range keys {
		err = builder.Insert([]byte(key), leafNodes[key])
		if err != nil {
			return err
		}
	}

	ivt.memoryHashMap = nil
	ivt.isMemory = false

	ivt.Logger.Trace("[Trace] invert Serialization Finish, Writing to : %v%v_invert.idx", segmentName, ivt.fieldName)
	ivt.Logger.Trace("[Trace] invert Serialization Finish, Writing to : %v%v_invert.fst", segmentName, ivt.fieldName)

	return nil
}

func (ivt *invert) destroy() {
	ivt.memoryHashMap = nil
}

func (ivt *invert) setIdxMmap(mmap *utils.Mmap) {
	ivt.idxMmap = mmap
}

//func (ivt *invert) setBtree(btdb *tree.BTreeDB) {
//	ivt.btree = btdb
//}

//func (ivt *invert) mergeInvert(inverts []*invert, segmentName string, btdb *tree.BTreeDB) error {
//	idxFileName := fmt.Sprintf("%v%v_invert.idx", segmentName, ivt.fieldName)
//	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
//	if err != nil {
//		return err
//	}
//	defer idxFd.Close()
//
//	fi, _ := idxFd.Stat()
//	totalOffset := int(fi.Size())
//
//	ivt.btree = btdb
//	type ivtMerge struct {
//		ivt    *invert
//		key    string
//		docids []utils.DocIdNode
//		pgnum  uint32
//		index  int
//	}
//
//	ivts := make([]ivtMerge, 0)
//
//	for _, i := range inverts {
//		if i.btree == nil {
//			continue
//		}
//
//		key, _, pgnum, index, ok := ivt.GetFirstKV()
//		if !ok {
//			continue
//		}
//
//		docIds, _ := ivt.queryTerm(key)
//		ivts = append(ivts, ivtMerge{
//			ivt:    i,
//			key:    key,
//			docids: docIds,
//			pgnum:  pgnum,
//			index:  index,
//		})
//	}
//
//	resflag := 0
//	for i := range ivts {
//		resflag = resflag | (1 << uint(i))
//	}
//	flag := 0
//	for flag != resflag {
//		maxkey := ""
//		meridxs := make([]int, 0)
//		for idx, ivt := range ivts {
//
//			if (flag>>uint(idx)&0x1) == 0 && maxkey < ivt.key {
//				maxkey = ivt.key
//				meridxs = make([]int, 0)
//				meridxs = append(meridxs, idx)
//				continue
//			}
//
//			if (flag>>uint(idx)&0x1) == 0 && maxkey == ivt.key {
//				meridxs = append(meridxs, idx)
//				continue
//			}
//
//		}
//
//		value := make([]utils.DocIdNode, 0)
//
//		for _, idx := range meridxs {
//			value = append(value, ivts[idx].docids...)
//
//			key, _, pgnum, index, ok := ivts[idx].ivt.GetNextKV(ivts[idx].key)
//			if !ok {
//				flag = flag | (1 << uint(idx))
//				continue
//			}
//
//			ivts[idx].key = key
//			ivts[idx].pgnum = pgnum
//			ivts[idx].index = index
//			ivts[idx].docids, ok = ivts[idx].ivt.queryTerm(key)
//
//		}
//
//		lens := len(value)
//		lenBufer := make([]byte, 8)
//		binary.LittleEndian.PutUint64(lenBufer, uint64(lens))
//		idxFd.Write(lenBufer)
//		buffer := new(bytes.Buffer)
//		err = binary.Write(buffer, binary.LittleEndian, value)
//		if err != nil {
//			ivt.Logger.Error("[ERROR] invert --> Merge :: Error %v", err)
//			return err
//		}
//		idxFd.Write(buffer.Bytes())
//		ivt.btree.Set(ivt.fieldName, maxkey, uint64(totalOffset))
//		totalOffset = totalOffset + 8 + lens*utils.DOCNODE_SIZE
//
//	}
//
//	ivt.memoryHashMap = nil
//	ivt.isMemory = false
//
//	return nil
//}

func (ivt *invert) mergeInvert(inverts []*invert, segmentName string, delBitMap *utils.Bitmap) error {
	// TODO 测试
	// 用于存放所有fst的迭代器
	mergeFSTNodes := make([]*FstNode, len(inverts))

	for index, i := range inverts {
		// 如果倒排不存在fst
		if i.fst == nil {
			continue
		}
		// 拿到fst中的最小key
		minKey, err := i.fst.GetMinKey()
		if err != nil {
			return err
		}
		// 拿到fst中的最大key
		maxKey, err := i.fst.GetMaxKey()
		if err != nil {
			return err
		}
		// append(maxKey, []byte("#")...), 是为了保证可以遍历到所有的key,默认情况下不会取到maxKey
		iter, err := i.fst.Iterator(minKey, append(maxKey, []byte("#")...))
		if err != nil {
			return err
		}
		key, _ := iter.Current()
		mergeFSTNodes[index] = &FstNode{
			Key:  string(key),
			ivt:  i,
			Iter: iter,
		}
	}
	// 合并fst
	err := ivt.mergeFSTIteratorList(segmentName, mergeFSTNodes)
	if err != nil {
		return err
	}
	return nil
}

/*****************************************************************************
*  function name : MergeFSTIteratorList
*  params :
*  return :
*
*  description : 合并k个fst
*
******************************************************************************/
func (ivt *invert) mergeFSTIteratorList(segmentName string, mergeFSTNodes []*FstNode) error {

	// TODO Test测试
	// 保存新段的倒排链
	idxFileName := fmt.Sprintf("%v%v_invert.idx", segmentName, ivt.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer idxFd.Close()

	fi, _ := idxFd.Stat()
	totalOffset := int(fi.Size())

	// 保存新段的fst倒排索引
	fstFileName := fmt.Sprintf("%v%v_invert.fst", segmentName, ivt.fieldName)
	fstFd, err := os.OpenFile(fstFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer fstFd.Close()

	// 生成fst builder用于批量写入文件
	builder, err := vellum.New(fstFd, nil)
	if err != nil {
		return err
	}
	// 保证builder正常关闭, 否则无法写入文件
	defer builder.Close()
	// 先使用小顶堆
	var fstHeap FstHeap
	heap.Init(&fstHeap)
	for _, node := range mergeFSTNodes {
		heap.Push(&fstHeap, node)
	}

	for fstHeap.Len() > 0 {
		// 需要将重复的key统一处理
		nodeList := make([]*FstNode, 1)
		nodeList[0] = heap.Pop(&fstHeap).(*FstNode)
		node := heap.Pop(&fstHeap).(*FstNode)
		for node.Key == nodeList[len(nodeList)-1].Key {
			nodeList = append(nodeList, node)
			if fstHeap.Len() > 0 {
				node = heap.Pop(&fstHeap).(*FstNode)
			} else {
				break
			}
		}

		// 如果fstHeap的长度为0,说明整个mergeFstNodes都相同
		// 如果fstHeap的长度大于0, 循环退出条件为node.Key != nodeList[len(nodeList)-1].Key
		if fstHeap.Len() > 0 {
			// 最后多抛出了一个，需要复位
			heap.Push(&fstHeap, node)
		}

		value := make([]utils.DocIdNode, 0)
		// 开始处理nodeList, 里面都是相同的key的node
		for _, node := range nodeList {
			docIds, _ := node.ivt.queryTerm(node.Key)
			value = append(value, docIds...)
			if node.Iter.Next() == nil {
				key, _ := node.Iter.Current()
				heap.Push(&fstHeap, &FstNode{
					Key:  string(key),
					ivt:  node.ivt,
					Iter: node.Iter,
				})
			}
		}
		// TODO 通过BitMap将value中已经删除的id剔除
		// 将新的倒排链写入文件
		lens := len(value)
		lenBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(lens))
		idxFd.Write(lenBuffer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			ivt.Logger.Error("[ERROR] invert --> Merge :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())
		builder.Insert([]byte(nodeList[0].Key), uint64(totalOffset))
		totalOffset = totalOffset + 8 + lens*utils.DOCNODE_SIZE
	}
	return nil
}

//func (ivt *invert) GetFirstKV() (string, uint64, uint32, int, bool) {
//
//	if ivt.btree == nil {
//		return "", 0, 0, 0, false
//	}
//	return ivt.btree.GetFirstKV(ivt.fieldName)
//}
//
//func (ivt *invert) GetNextKV(key string) (string, uint64, uint32, int, bool) {
//
//	if ivt.btree == nil {
//		return "", 0, 0, 0, false
//	}
//
//	return ivt.btree.GetNextKV(ivt.fieldName, key)
//}

func (ivt *invert) queryTerm(keyStr string) ([]utils.DocIdNode, bool) {

	if ivt.isMemory == true {
		docIds, ok := ivt.memoryHashMap[keyStr]
		if ok {
			return docIds, true
		}
	} else if ivt.idxMmap != nil {
		// ok, offset := ivt.btree.Search(ivt.fieldName, keyStr)
		offset, ok, err := ivt.fst.Get([]byte(keyStr))
		if !ok {
			return nil, false
		}
		if err != nil {
			ivt.Logger.Error("[Error] queryTerm fail")
		}
		lens := ivt.idxMmap.ReadInt64(int64(offset))

		res := ivt.idxMmap.ReadDocIdsArry(uint64(offset)+8, uint64(lens))
		return res, true
	}

	return nil, false
}
