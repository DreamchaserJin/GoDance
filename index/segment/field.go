/**
 * @Author hz
 * @Date 6:08 AM$ 5/21/22$
 * @Note 字段类型以及相关的方法
 **/

package segment

import (
	"errors"
	"fmt"
	"gdindex/tree"
	"utils"
)

type SimpleFieldInfo struct {
	FieldName string `json:"fieldName"`
	FieldType uint32 `json:"fieldType"`
}

type Field struct {
	fieldName  string
	startDocId uint32
	maxDocId   uint32
	fieldType  uint32
	isMemory   bool
	ivt        *invert
	pfl        *profile
	pfi        *profileindex
	pfiMmap    *utils.Mmap
	idxMmap    *utils.Mmap
	pflMmap    *utils.Mmap
	dtlMmap    *utils.Mmap

	btree *tree.BTreeDB

	Logger *utils.Log4FE `json:"-"`
}

func newEmptyFakeField(fieldName string, start, cur, fieldType uint32, logger *utils.Log4FE) *Field {
	f := &Field{
		fieldName:  fieldName,
		startDocId: start,
		maxDocId:   cur,
		fieldType:  fieldType,
		Logger:     logger,
		isMemory:   false,
	}

	f.pfl = newEmptyFakeProfile(fieldName, fieldType, start, cur, logger)
	return f
}

func newEmptyField(fieldName string, start, fieldType uint32, logger *utils.Log4FE) *Field {
	f := &Field{
		fieldName:  fieldName,
		startDocId: start,
		maxDocId:   start,
		fieldType:  fieldType,
		isMemory:   true,
		Logger:     logger,
	}

	if fieldType == utils.IDX_TYPE_STRING ||
		fieldType == utils.IDX_TYPE_STRING_SEG {
		f.ivt = newEmptyInvert(fieldType, start, fieldName, logger)
	}
	if fieldType == utils.IDX_TYPE_NUMBER ||
		fieldType == utils.IDX_TYPE_DATE ||
		fieldType == utils.IDX_TYPE_FLOAT {
		f.pfi = newEmptyProfileIndex(fieldType, start, fieldName, logger)
	}

	f.pfl = newEmptyProfile(fieldName, fieldType, start, logger)

	return f
}

func newFieldFromLocalFile(fieldName, segmentName string, start, max uint32,
	fieldType uint32, btree *tree.BTreeDB, logger *utils.Log4FE) *Field {

	f := &Field{
		fieldName:  fieldName,
		startDocId: start,
		maxDocId:   max,
		fieldType:  fieldType,
		isMemory:   false,
		btree:      btree,
		Logger:     logger,
	}
	var err error
	f.idxMmap, err = utils.NewMmap(fmt.Sprintf("%v%v_invert.idx", segmentName, f.fieldName), utils.MODE_APPEND)
	if err != nil {
		f.Logger.Error("[ERROR] Mmap error : %v", err)
	}
	f.idxMmap.SetFileEnd(0)

	f.pflMmap, err = utils.NewMmap(fmt.Sprintf("%v%v_profile.pfl", segmentName, f.fieldName), utils.MODE_APPEND)
	if err != nil {
		f.Logger.Error("[ERROR] Mmap error : %v", err)
	}
	f.pflMmap.SetFileEnd(0)

	f.dtlMmap, err = utils.NewMmap(fmt.Sprintf("%v%v_detail.dtl", segmentName, f.fieldName), utils.MODE_APPEND)
	if err != nil {
		f.Logger.Error("[ERROR] Mmap error : %v", err)
	}
	f.dtlMmap.SetFileEnd(0)

	f.pfiMmap, err = utils.NewMmap(fmt.Sprintf("%v%v_profileindex.pfi", segmentName, f.fieldName), utils.MODE_APPEND)
	if err != nil {
		f.Logger.Error("[ERROR] Mmap error : %v", err)
	}
	f.pfiMmap.SetFileEnd(0)

	f.Logger.Info("[INFO] Field %v Serialization Finish", f.fieldName)
	if fieldType == utils.IDX_TYPE_STRING ||
		fieldType == utils.IDX_TYPE_STRING_SEG {
		f.ivt = newInvertFromLocalFile(btree, fieldType, fieldName, segmentName, f.idxMmap, logger)
	}

	if fieldType == utils.IDX_TYPE_NUMBER ||
		fieldType == utils.IDX_TYPE_DATE ||
		fieldType == utils.IDX_TYPE_FLOAT {
		f.pfi = newProfileIndexFromLocalFile(btree, fieldType, fieldName, segmentName, f.pfiMmap, logger)
	}

	f.pfl = newProfileFromLocalFile(fieldName, fieldType, f.startDocId, f.maxDocId, f.pflMmap, f.dtlMmap, logger)

	f.setMmap()

	return f
}

func (f *Field) addDocument(docId uint32, contentStr string) error {
	if docId != f.maxDocId || f.isMemory == false || f.pfl == nil {
		f.Logger.Error("[ERROR] Field  AddDocument :: Wrong docid %v this.maxDocId %v this.profile %v", docId, f.maxDocId, f.pfl)
		return errors.New("[ERROR] Wrong docid")
	}

	if err := f.pfl.addDocument(docId, contentStr); err != nil {
		f.Logger.Error("[ERROR] Field AddDocument :: Add Document Error %v", err)
		return err
	}

	if f.fieldType != utils.IDX_TYPE_NUMBER &&
		f.fieldType != utils.IDX_TYPE_DATE &&
		f.fieldType != utils.IDX_TYPE_FLOAT &&
		f.ivt != nil {
		if err := f.ivt.addDocument(docId, contentStr); err != nil {
			f.Logger.Error("[ERROR] Field AddDocument :: Add Invert Document Error %v", err)
			return err
		}
	}

	if (f.fieldType == utils.IDX_TYPE_NUMBER ||
		f.fieldType == utils.IDX_TYPE_DATE ||
		f.fieldType == utils.IDX_TYPE_FLOAT) &&
		f.pfi != nil {
		if err := f.pfi.addDocument(docId, contentStr); err != nil {
			f.Logger.Error("[ERROR] Field --> AddDocument :: Add ProfileIndex Document Error %v", err)
			return err
		}
	}

	f.maxDocId++
	return nil
}

func (f *Field) query(key string) ([]utils.DocIdNode, bool) {
	if f.ivt == nil {
		return nil, false
	}

	return f.ivt.queryTerm(fmt.Sprintf("%v", key))
}

func (f *Field) queryFilter(filter utils.SearchFilters) ([]uint32, bool) {
	if f.pfi == nil {
		return nil, false
	}

	var start, end int64

	switch filter.Type {
	case utils.FILT_EQ:
		start = filter.Start
		end = filter.Start
	case utils.FILT_RANGE:
		start = filter.Start
		end = filter.End
	case utils.FILT_LESS:
		start = 0
		end = filter.End
	case utils.FILT_OVER:
		start = filter.Start
		end = 0xFFFFFFFFFF
	}

	return f.pfi.queryRange(start, end)
}

func (f *Field) getValue(docId uint32) (string, bool) {
	if docId >= f.startDocId && docId < f.maxDocId && f.pfl != nil {
		return f.pfl.getValue(docId - f.startDocId)
	}

	return "", false
}

func (f *Field) serialization(segmentName string, btdb *tree.BTreeDB) error {

	if f.pfl != nil {
		err := f.pfl.serialization(segmentName)
		if err != nil {
			f.Logger.Error("[ERROR] Field Serialization Error : %v", err)
			return err
		}
	}

	if f.pfi != nil {
		f.btree = btdb
		if err := f.btree.AddBTree(f.fieldName); err != nil {
			f.Logger.Error("[ERROR] Field Serialization, Create BPTree ERROR : %v", err)
			return err
		}
		err := f.pfi.serialization(segmentName, f.btree)
		if err != nil {
			f.Logger.Error("[ERROR] Field Serialization Error : %v", err)
			return err
		}
	}

	if f.ivt != nil {
		err := f.ivt.serialization(segmentName, f.btree)
		if err != nil {
			f.Logger.Error("[ERROR] Field Serialization Error : %v", err)
			return err
		}
	}

	var err error
	f.idxMmap, err = utils.NewMmap(fmt.Sprintf("%v%v_invert.idx", segmentName, f.fieldName), utils.MODE_APPEND)
	if err != nil {
		f.Logger.Error("[ERROR] Mmap error : %v", err)
	}
	f.idxMmap.SetFileEnd(0)
	f.Logger.Debug("[INFO] Load Invert File : %v%v_invert.idx", segmentName, f.fieldName)

	f.pflMmap, err = utils.NewMmap(fmt.Sprintf("%v%v_profile.pfl", segmentName, f.fieldName), utils.MODE_APPEND)
	if err != nil {
		f.Logger.Error("[ERROR] Mmap error : %v", err)
	}
	f.pflMmap.SetFileEnd(0)

	f.pfiMmap, err = utils.NewMmap(fmt.Sprintf("%v%v_profileindex.pfi", segmentName, f.fieldName), utils.MODE_APPEND)
	if err != nil {
		f.Logger.Error("[ERROR] Mmap error : %v", err)
	}
	f.pfiMmap.SetFileEnd(0)

	f.dtlMmap, err = utils.NewMmap(fmt.Sprintf("%v%v_detail.dtl", segmentName, f.fieldName), utils.MODE_APPEND)
	if err != nil {
		f.Logger.Error("[ERROR] Mmap error : %v", err)
	}
	f.dtlMmap.SetFileEnd(0)

	f.setMmap()

	f.Logger.Info("[INFO] Field %v Serialization Finish", f.fieldName)

	return nil
}

func (f *Field) destroy() {
	if f.pfl != nil {
		f.pfl.destroy()
	}

	if f.pfi != nil {
		f.pfi.destroy()
	}

	if f.ivt != nil {
		f.ivt.destroy()
	}
}

func (f *Field) mergeField(fields []*Field, segmentName string, btree *tree.BTreeDB, delDocSet map[uint32]struct{}) error {

	if f.pfl != nil {
		pfls := make([]*profile, 0)

		for _, fd := range fields {
			pfls = append(pfls, fd.pfl)
		}

		docSize, err := f.pfl.mergeProfiles(pfls, segmentName, delDocSet)
		if err != nil {
			f.Logger.Error("[Error] Field %v merge Error : %v", f.fieldName, err)
			return err
		}

		f.maxDocId += docSize
	}

	if f.pfi != nil {
		f.btree = btree
		if err := f.btree.AddBTree(f.fieldName); err != nil {
			f.Logger.Error("[ERROR] field %v Create Btree Error : %v", f.fieldName, err)
			return err
		}
		pfis := make([]*profileindex, 0)
		for _, fd := range fields {
			if fd.pfi != nil {
				pfis = append(pfis, fd.pfi)
			} else {
				f.Logger.Error("[INFO] Invert %v is nil")
			}
		}
		if err := f.pfi.mergeProfileIndex(pfis, segmentName, btree); err != nil {
			return err
		}
	}

	if f.ivt != nil {
		// TODO 考虑删除，为字段倒排新建数据表
		f.btree = btree
		if err := f.btree.AddBTree(f.fieldName); err != nil {
			f.Logger.Error("[ERROR] Invert %v Create Btree Error : %v", f.fieldName, err)
			return err
		}
		ivts := make([]*invert, 0)
		for _, fd := range fields {
			if fd.ivt != nil {
				ivts = append(ivts, fd.ivt)
			} else {
				f.Logger.Error("[INFO] Invert %v is nil")
			}
		}
		if err := f.ivt.mergeInvert(ivts, segmentName); err != nil {
			return err
		}
	}

	return nil
}

func (f *Field) setMmap() {
	f.setIdxMmap(f.idxMmap)
	f.setPflMmap(f.pflMmap)
	f.setDtlMmap(f.dtlMmap)
	f.setPfiMmap(f.pfiMmap)
}

func (f *Field) setIdxMmap(idxMmap *utils.Mmap) {
	if f.ivt != nil {
		f.ivt.setIdxMmap(idxMmap)
	}
}

func (f *Field) setPflMmap(pflMmap *utils.Mmap) {
	if f.pfl != nil {
		f.pfl.setPflMmap(pflMmap)
	}
}

func (f *Field) setPfiMmap(pflidxMmap *utils.Mmap) {
	if f.pfi != nil {
		f.pfi.setPfiMmap(pflidxMmap)
	}
}

func (f *Field) setDtlMmap(dtlMmap *utils.Mmap) {
	if f.pfl != nil {
		f.pfl.setDtlMmap(dtlMmap)
	}
}
