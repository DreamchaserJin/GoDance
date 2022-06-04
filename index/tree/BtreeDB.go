package tree

import (
	"GoDance/utils"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"strconv"
)

type BTreeDB struct {
	filename string
	dbHelper *BoltHelper
	buckets  map[string]*bolt.Tx
	logger   *utils.Log4FE
}

func NewBTDB(dbname string, logger *utils.Log4FE) *BTreeDB {

	this := &BTreeDB{filename: dbname, dbHelper: nil, logger: logger, buckets: make(map[string]*bolt.Tx)}
	this.dbHelper = NewBoltHelper(dbname, logger)

	return this
}

func (db *BTreeDB) AddBTree(name string) error {
	_, err := db.dbHelper.CreateBtree(name)
	return err
}

func (db *BTreeDB) Set(btname string, key int64, value uint64) error {
	return db.dbHelper.Set(btname, key, fmt.Sprintf("%v", value))
}

func (db *BTreeDB) SetBatch(btname string, kv map[int64]string) error {
	return db.dbHelper.SetBatch(btname, kv)
}

func (db *BTreeDB) Search(btname string, key int64) (bool, uint64) {

	//db.logger.Info("Search btname : %v  key : %v  ",btname,key)
	vstr, err := db.dbHelper.Get(btname, key)
	if err != nil {
		return false, 0
	}
	//db.logger.Info("Search btname : %v  key : %v value str : %v ",btname,key,vstr)
	res, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return false, 0
	}
	//db.logger.Info("Search btname : %v  key : %v value  : %v ",btname,key,u)
	return true, res
}

func (db *BTreeDB) SearchRange(btname string, keyMin, keyMax int64) (bool, []uint64) {

	if keyMin > keyMax {
		return false, nil
	}

	vstr, err := db.dbHelper.GetRange(btname, keyMin, keyMax)
	if err != nil {
		return false, nil
	}

	res := make([]uint64, 0, 10)
	for _, v := range vstr {
		u, e := strconv.ParseUint(v, 10, 64)
		if e != nil {
			return false, nil
		}
		res = append(res, u)
	}
	return true, res
}

func (db *BTreeDB) GetFirstKV(btname string) (int64, uint64, bool) {

	key, vstr, err := db.dbHelper.GetFirstKV(btname)
	if err != nil {
		return -1, 0, false
	}
	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return -1, 0, false
	}
	buf := bytes.NewBuffer(key)
	var kv int64
	binary.Read(buf, binary.BigEndian, &kv)

	return kv, u, true
}

func (db *BTreeDB) GetNextKV(btname string, key int64) (int64, uint64, bool) {

	vkey, vstr, err := db.dbHelper.GetNextKV(btname, key)
	if err != nil {
		return -1, 0, false
	}

	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return -1, 0, false
	}

	buf := bytes.NewBuffer(vkey)
	var kv int64
	binary.Read(buf, binary.BigEndian, &kv)
	return kv, u, true

}

func (db *BTreeDB) Close() error {
	return db.dbHelper.CloseDB()
}

func (db *BTreeDB) MergeBTrees() error {

	return nil
}
