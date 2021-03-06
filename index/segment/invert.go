/**
 * @Author iceberg
 * @Date 6:20 AM$ 5/21/22$
 * @Note
 **/

package segment

import (
	"GoDance/search/weight"
	"GoDance/utils"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	"os"
	"sort"
)

type invert struct {
	curDocId      uint64
	isMemory      bool
	fieldType     uint64
	fieldName     string
	idxMmap       *utils.Mmap
	memoryHashMap map[string][]utils.DocIdNode
	Logger        *utils.Log4FE
	fst           *vellum.FST
}

func newEmptyInvert(fieldType uint64, startDocId uint64, fieldName string, logger *utils.Log4FE) *invert {
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

func newInvertFromLocalFile(fieldType uint64, fieldName, segmentName string,
	idxMmap *utils.Mmap, logger *utils.Log4FE) *invert {
	ivt := &invert{
		isMemory:  false,
		fieldType: fieldType,
		fieldName: fieldName,
		idxMmap:   idxMmap,
		Logger:    logger,
		fst:       nil,
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

// 添加文档
func (ivt *invert) addDocument(docId uint64, contentStr string) error {
	var segResult []string
	// 判断文本类型，根据类型不同选择不同的分词策略
	if ivt.fieldType == utils.IDX_TYPE_STRING {
		segResult = []string{contentStr}
	} else if ivt.fieldType == utils.IDX_TYPE_STRING_SEG {
		segmenter := utils.GetGseSegmenter()
		segResult = segmenter.CutSearch(contentStr, false)
	} else {
		return errors.New("invert fieldType is not exists")
	}
	// 计算权重
	tf := weight.TF(segResult)
	// memoryHashMap判空
	if ivt.memoryHashMap == nil {
		ivt.memoryHashMap = make(map[string][]utils.DocIdNode)
	}
	// set集合用于存储不重复的分词
	segset := make(map[string]struct{})
	for _, val := range segResult {
		// 对分词结果进行去重
		if _, ok := segset[val]; !ok {
			segset[val] = struct{}{}
			docIdNode := utils.DocIdNode{Docid: docId, WordTF: tf[val]}
			ivt.memoryHashMap[val] = append(ivt.memoryHashMap[val], docIdNode)
		}
	}
	return nil
}

//
//  serialization
//  @Description:
//  @receiver ivt
//  @param segmentName
//  @param btree
//  @return error
//
func (ivt *invert) serialization(segmentName string) error {
	// fst存储文件名
	fstFileName := fmt.Sprintf("%v%v_invert.fst", segmentName, ivt.fieldName)
	// 打开fst文件
	fstFd, err := os.OpenFile(fstFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	// 保证fst文件可以关闭
	defer fstFd.Close()

	// 打开idx文件，用于存储memoryHashMap, 一个倒排字典
	idxFileName := fmt.Sprintf("%v%v_invert.idx", segmentName, ivt.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	fi, _ := idxFd.Stat()
	nowOffset := uint64(fi.Size())

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
	defer func(builder *vellum.Builder) {
		err := builder.Close()
		if err != nil {

		}
	}(builder)

	leafNodes := make(map[string]uint64)

	// 因为插入fst的key必须是有序的,所以需要记录memoryHashMap中的key值，以供排序
	keys := make([]string, len(ivt.memoryHashMap))

	for key, value := range ivt.memoryHashMap {

		lens := len(value)

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
		keys = append(keys, key)

		// 不用b+树存倒排索引了
		//ivt.btree.Set(ivt.fieldName, key, nowOffset)

		nowOffset += uint64(lens*utils.DOCNODE_SIZE) + 8
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

func (ivt *invert) mergeInvert(inverts []*invert, segmentName string) error {

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

	ivt.memoryHashMap = nil
	ivt.isMemory = false

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

	// 使用小顶堆
	var fstHeap FstHeap
	heap.Init(&fstHeap)
	for _, node := range mergeFSTNodes {
		heap.Push(&fstHeap, node)
	}

	for fstHeap.Len() > 0 {
		// 需要将重复的key统一处理
		nodeList := make([]*FstNode, 0)
		nodeList = append(nodeList, heap.Pop(&fstHeap).(*FstNode))
		var node *FstNode

		for fstHeap.Len() > 0 {
			node = heap.Pop(&fstHeap).(*FstNode)
			if node != nil && node.Key == nodeList[len(nodeList)-1].Key {
				nodeList = append(nodeList, node)
			} else if node.Key != nodeList[len(nodeList)-1].Key {
				heap.Push(&fstHeap, node)
				break
			} else {
				break
			}
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
