/**
 * @Author hz
 * @Date 6:52 AM$ 5/21/22$
 * @Note
 **/

package tree

import (
	"fmt"
	"github.com/boltdb/bolt"
	"os"
	"strconv"
	"utils"
)

type BTreeDB struct {
	filename  string
	mmapBytes []byte

	fd *os.File

	dbHelper *utils.BoltHelper
	buckets  map[string]*bolt.Tx
	logger   *utils.Log4FE
}

func NewBTDB(btdbName string, logger *utils.Log4FE) *BTreeDB {
	db := &BTreeDB{
		filename: btdbName,
		dbHelper: nil,
		buckets:  make(map[string]*bolt.Tx),
		logger:   logger,
	}

	db.dbHelper = utils.NewBoltHelper(btdbName, 0, logger)

	return db
}

func (db *BTreeDB) AddTree(name string) error {
	_, err := db.dbHelper.CreateTable(name)
	return err
}

func (db *BTreeDB) Set(btName, key string, value uint64) error {
	return db.dbHelper.Update(btName, key, fmt.Sprintf("%v", value))
}

func (db *BTreeDB) MultiSet(btName string, kv map[string]string) error {
	return db.dbHelper.SetBatch(btName, kv)
}

func (db *BTreeDB) Search(btName, key string) (bool, uint64) {
	vstr, err := db.dbHelper.Get(btName, key)
	if err != nil {
		return false, 0
	}

	offset, err := strconv.ParseUint(vstr, 10, 64)
	if err != nil {
		return false, 0
	}

	return true, offset
}

func (db *BTreeDB) GetFirstKV(btname string) (string, uint64, uint32, int, bool) {
	key, valStr, err := db.dbHelper.GetFristKV(btname)
	if err != nil {
		return "", 0, 0, 0, false
	}

	valInt64, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return "", 0, 0, 0, false
	}

	return key, valInt64, 0, 0, true
}

func (db *BTreeDB) GetNextKV(btname string, key string) (string, uint64, uint32, int, bool) {
	vkey, valStr, err := db.dbHelper.GetNextKV(btname, key)
	if err != nil {
		return "", 0, 0, 0, false
	}

	valInt64, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return "", 0, 0, 0, false
	}

	return vkey, valInt64, 0, 0, true
}

func (db *BTreeDB) Close() error {
	return db.dbHelper.CloseDB()
}
