/**
 * @Author hz
 * @Date 6:20 AM$ 5/21/22$
 * @Note 文档仓 用于存储所有文档
 **/

package segment

import (
	"GoDance/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
)

type profile struct {
	startDocId uint64
	maxDocId   uint64
	isMemory   bool
	fake       bool
	fieldType  uint64
	fieldName  string

	pflNumber []int64
	pflString []string
	pflFloat  []float64
	pflMmap   *utils.Mmap
	dtlMmap   *utils.Mmap

	Logger *utils.Log4FE `json:"-"`
}

//  newEmptyFakeProfile
//  @Description: 新建一个假的正排索引，用于在段合并时某些段缺少某些字段时使用
//  @param fieldName 字段名
//  @param fieldType 字段类型
//  @param start  起始文档ID
//  @param cur  最大文档ID
//  @return *profile 正排索引
func newEmptyFakeProfile(fieldName string, fieldType, start, cur uint64, logger *utils.Log4FE) *profile {
	pfl := &profile{
		startDocId: start,
		maxDocId:   cur,
		isMemory:   false,
		fake:       true,
		fieldType:  fieldType,
		fieldName:  fieldName,
		pflNumber:  make([]int64, 0),
		pflString:  make([]string, 0),
		Logger:     logger,
	}

	return pfl
}

//  newEmptyProfile
//  @Description: 创建一个新的正排索引
//  @param fieldName 字段名
//  @param fieldType 字段类型
//  @param start 起始文档ID
//  @return *profile 新的正排索引
func newEmptyProfile(fieldName string, fieldType, start uint64, logger *utils.Log4FE) *profile {
	pfl := &profile{
		startDocId: start,
		maxDocId:   start,
		isMemory:   true,
		fake:       false,
		fieldType:  fieldType,
		fieldName:  fieldName,
		pflNumber:  make([]int64, 0),
		pflString:  make([]string, 0),
		Logger:     logger,
	}
	return pfl
}

//
//  newProfileFromLocalFile
//  @Description: 反序列化正排索引
//  @param fieldName 字段名
//  @param fieldType 字段类型
//  @param start 起始文档ID
//  @param cur 最大文档ID
//  @param pflMmap 正排文件的Mmap
//  @param dtlMmap 字段内容的Mmap
//  @return *profile 正排对象
//
func newProfileFromLocalFile(fieldName string, fieldType, start, cur uint64, pflMmap, dtlMmap *utils.Mmap, logger *utils.Log4FE) *profile {
	pfl := &profile{
		isMemory:   false,
		startDocId: start,
		maxDocId:   cur,
		fieldType:  fieldType,
		fieldName:  fieldName,
		pflMmap:    pflMmap,
		dtlMmap:    dtlMmap,
		Logger:     logger,
	}
	return pfl
}

//  addDocument
//  @Description 新增文档
//  @param docId 文档ID
//  @param contentStr 内容
//  @return error 任何错误
func (pfl *profile) addDocument(docId uint64, contentStr string) error {
	if docId != pfl.maxDocId || pfl.isMemory == false {
		return errors.New("profile AddDocument :: Wrong DocId Number")
	}
	pfl.Logger.Trace("[TRACE] docId %v content %v", docId, contentStr)

	// 最终存进去的值，如果是 -1 则表示空值
	var value int64 = -1
	var err error
	if pfl.fieldType == utils.IDX_TYPE_NUMBER {
		value, err = strconv.ParseInt(contentStr, 10, 64)
		if err != nil {
			value = -1
		}
		pfl.pflNumber = append(pfl.pflNumber, value)

	} else if pfl.fieldType == utils.IDX_TYPE_DATE {

		value, _ = utils.IsDateTime(contentStr)
		pfl.pflNumber = append(pfl.pflNumber, value)

	} else if pfl.fieldType == utils.IDX_TYPE_FLOAT {
		f, err := strconv.ParseFloat(contentStr, 64)
		if err != nil {
			value = -1
		}
		value = int64(f * 100)
		pfl.pflNumber = append(pfl.pflNumber, value)
	} else {
		pfl.pflString = append(pfl.pflString, contentStr)
	}

	return nil
}

//
//  serialization
//  @Description: 正排索引序列化
//  @param segmentName 段名
//  @return error 任何错误
//
func (pfl *profile) serialization(segmentName string) error {

	pflFileName := fmt.Sprintf("%v%v_profile.pfl", segmentName, pfl.fieldName)

	pflFd, err := os.OpenFile(pflFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer pflFd.Close()

	if pfl.fieldType == utils.IDX_TYPE_NUMBER || pfl.fieldType == utils.IDX_TYPE_DATE ||
		pfl.fieldType == utils.IDX_TYPE_FLOAT {
		valueBuffer := make([]byte, 8)

		for _, info := range pfl.pflNumber {
			binary.LittleEndian.PutUint64(valueBuffer, uint64(info))
			_, err := pflFd.Write(valueBuffer)
			if err != nil {
				pfl.Logger.Error("[ERROR] NumberProfiles --> Serialization :: Write Error %v", err)
			}
		}
	} else {
		dtlFileName := fmt.Sprintf("%v%v_detail.dtl", segmentName, pfl.fieldName)

		dtlFd, err := os.OpenFile(dtlFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		defer dtlFd.Close()
		lenBuffer := make([]byte, 8)
		var nowOffset int64 = 0
		for _, value := range pfl.pflString {
			valueLen := len(value)
			binary.LittleEndian.PutUint64(lenBuffer, uint64(valueLen))
			_, err := dtlFd.Write(lenBuffer)
			cnt, err := dtlFd.WriteString(value)

			if err != nil || cnt != valueLen {
				pfl.Logger.Error("[ERROR] StringProfile Write Error : %v", err)
			}

			binary.LittleEndian.PutUint64(lenBuffer, uint64(nowOffset))
			_, err = pflFd.Write(lenBuffer)
			if err != nil {
				pfl.Logger.Error("[ERROR] StringProfile Write Error : %v", err)
			}
			nowOffset = nowOffset + int64(valueLen) + 8
		}
	}

	pfl.isMemory = false
	pfl.pflNumber = nil
	pfl.pflString = nil

	return err
}

//
//  destroy
//  @Description 回收内存
//
func (pfl *profile) destroy() {
	pfl.pflString = nil
	pfl.pflNumber = nil
}

func (pfl *profile) setPflMmap(pflMmap *utils.Mmap) {
	pfl.pflMmap = pflMmap
}

func (pfl *profile) setDtlMmap(dtlMmap *utils.Mmap) {
	pfl.dtlMmap = dtlMmap
}

//
//  mergeProfiles
//  @Description 合并正排对象
//  @param profiles 需要合并的正排对象
//  @param segmentName 段名
//  @return uint32 文档长度
//  @return error 任何错误
func (pfl *profile) mergeProfiles(profiles []*profile, segmentName string, delDocSet map[uint64]struct{}) (uint64, error) {
	pflFileName := fmt.Sprintf("%v%v_profile.pfl", segmentName, pfl.fieldName)

	pflFd, err := os.OpenFile(pflFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return 0, err
	}

	defer pflFd.Close()
	var lens uint64

	if pfl.fieldType == utils.IDX_TYPE_NUMBER || pfl.fieldType == utils.IDX_TYPE_DATE ||
		pfl.fieldType == utils.IDX_TYPE_FLOAT {
		valBuffer := make([]byte, 8)
		for _, p := range profiles {
			for i := uint64(0); i < (p.maxDocId - p.startDocId); i++ {
				val, _ := p.getIntValue(i)
				binary.LittleEndian.PutUint64(valBuffer, uint64(val))
				_, err := pflFd.Write(valBuffer)
				if err != nil {
					pfl.Logger.Error("[ERROR] StringProfile Write Error : %v", err)
				}
				pfl.maxDocId++
			}
		}
		lens = pfl.maxDocId - pfl.startDocId
	} else {
		dtlFileName := fmt.Sprintf("%v%v_detail.pfl", segmentName, pfl.fieldName)
		dtlFd, err := os.OpenFile(dtlFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return 0, err
		}
		defer dtlFd.Close()
		fi, _ := dtlFd.Stat()
		dtlOffset := fi.Size()

		lenBuffer := make([]byte, 8)
		for _, p := range profiles {
			for i := uint64(0); i < (p.maxDocId - p.startDocId); i++ {
				if _, ok := delDocSet[p.startDocId+i]; ok {

					binary.LittleEndian.PutUint64(lenBuffer, uint64(0))
					_, err := dtlFd.Write(lenBuffer)
					if err != nil {
						pfl.Logger.Error("[ERROR] StringProfile Write Error : %v", err)
					}

					binary.LittleEndian.PutUint64(lenBuffer, uint64(dtlOffset))
					_, err = pflFd.Write(lenBuffer)
					if err != nil {
						pfl.Logger.Error("[ERROR] StringProfile Write Error : %v", err)
					}
					dtlOffset += 8
					pfl.maxDocId++

					continue
				}

				val, _ := p.getValue(i)
				valLen := len(val)
				binary.LittleEndian.PutUint64(lenBuffer, uint64(valLen))
				_, err := dtlFd.Write(lenBuffer)
				cnt, err := dtlFd.WriteString(val)
				if err != nil || cnt != valLen {
					pfl.Logger.Error("[ERROR] StringProfile Write Error : %v", err)
				}

				binary.LittleEndian.PutUint64(lenBuffer, uint64(dtlOffset))
				_, err = pflFd.Write(lenBuffer)
				if err != nil {
					pfl.Logger.Error("[ERROR] StringProfile Write Error : %v", err)
				}
				dtlOffset += int64(valLen) + 8
				pfl.maxDocId++
			}
		}
		lens = pfl.maxDocId - pfl.startDocId
	}
	pfl.isMemory = false
	pfl.pflString = nil
	pfl.pflNumber = nil

	return lens, nil
}

// getValue
// @Description
// @Param pos
// @Return string
// @Return bool
func (pfl *profile) getValue(pos uint64) (string, bool) {
	if pfl.fake {
		return "", true
	}

	if pfl.isMemory && pos < uint64(len(pfl.pflNumber)) {
		if pfl.fieldType == utils.IDX_TYPE_NUMBER {
			return fmt.Sprintf("%v", pfl.pflNumber[pos]), true
		} else if pfl.fieldType == utils.IDX_TYPE_DATE {
			return utils.FormatDateTime(pfl.pflNumber[pos])
		} else if pfl.fieldType == utils.IDX_TYPE_FLOAT {
			return fmt.Sprintf("%v", float64(pfl.pflNumber[pos])/100), true
		}
		return pfl.pflString[pos], true
	}

	if pfl.pflMmap == nil {
		return "", false
	}

	offset := int64(pos) * 8
	if pfl.fieldType == utils.IDX_TYPE_NUMBER {
		return fmt.Sprintf("%v", pfl.pflMmap.ReadInt64(offset)), true
	} else if pfl.fieldType == utils.IDX_TYPE_DATE {
		return utils.FormatDateTime(pfl.pflMmap.ReadInt64(offset))
	} else if pfl.fieldType == utils.IDX_TYPE_FLOAT {
		val := pfl.pflMmap.ReadInt64(offset)
		return fmt.Sprintf("%v", float64(val)/100), true
	}

	if pfl.dtlMmap == nil {
		return "", false
	}

	dtlOffset := pfl.pflMmap.ReadInt64(offset)
	lens := pfl.dtlMmap.ReadInt64(dtlOffset)
	return pfl.dtlMmap.ReadString(dtlOffset+8, lens), true
}

// getIntValue
// @Description
// @Param pos
// @Return int64
// @Return bool
func (pfl *profile) getIntValue(pos uint64) (int64, bool) {
	if pfl.fake {
		return -1, true
	}

	if pfl.isMemory {
		if (pfl.fieldType == utils.IDX_TYPE_NUMBER || pfl.fieldType == utils.IDX_TYPE_DATE ||
			pfl.fieldType == utils.IDX_TYPE_FLOAT) && pos < uint64(len(pfl.pflNumber)) {
			return pfl.pflNumber[pos], true
		}
		return -1, false
	}

	if pfl.pflMmap == nil {
		return -1, true
	}

	offset := int64(pos) * 8
	if pfl.fieldType == utils.IDX_TYPE_NUMBER || pfl.fieldType == utils.IDX_TYPE_DATE ||
		pfl.fieldType == utils.IDX_TYPE_FLOAT {
		return pfl.pflMmap.ReadInt64(offset), true
	}

	return -1, false
}
