package tree

import (
	"GoDance/utils"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
)

type BoltHelper struct {
	name   string
	db     *bolt.DB
	Logger *utils.Log4FE
}

func NewBoltHelper(dbname string, logger *utils.Log4FE) *BoltHelper {
	var err error
	this := &BoltHelper{name: dbname, Logger: logger}
	this.db, err = bolt.Open(dbname, 0644, nil)
	if err != nil {
		this.Logger.Error("[ERROR] Open Dbname Error %v", err)
	}

	return this
}

func (bh *BoltHelper) CreateBtree(btName string) (*bolt.Bucket, error) {

	tx, err := bh.db.Begin(true)
	if err != nil {
		bh.Logger.Error("[ERROR] Create Tx Error %v ", err)
		return nil, err
	}
	defer tx.Rollback()

	table, err := tx.CreateBucketIfNotExists([]byte(btName))
	if err != nil {
		bh.Logger.Error("[ERROR] Create Table Error %v", err)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		bh.Logger.Error("[ERROR] Commit Tx Error %v", err)
		return nil, err
	}

	return table, nil
}

func (bh *BoltHelper) DeleteBtree(btName string) error {

	tx, err := bh.db.Begin(true)
	if err != nil {
		bh.Logger.Error("[ERROR] DeleteTable Tx Error %v ", err)
		return err
	}
	defer tx.Rollback()
	//func (*Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error)
	err = tx.DeleteBucket([]byte(btName))
	if err != nil {
		bh.Logger.Warn("[WARN] DeleteTable Table Error %v", err)
	}

	if err := tx.Commit(); err != nil {
		bh.Logger.Error("[ERROR] Commit Tx Error %v", err)
		return err
	}

	return nil

}

func (bh *BoltHelper) GetBtree(btName string) (*bolt.Bucket, error) {

	tx, err := bh.db.Begin(true)
	if err != nil {
		bh.Logger.Error("[ERROR] Create Tx Error %v ", err)
		return nil, err
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(btName))

	if b == nil {
		bh.Logger.Error("[ERROR] Tablename[%v] not found", btName)
		return nil, fmt.Errorf("Tablename[%v] not found", btName)
	}

	if err := tx.Commit(); err != nil {
		bh.Logger.Error("[ERROR] Commit Tx Error %v", err)
		return nil, err
	}

	return b, nil
}

// Set function description : 更新数据
// params :
// return :
func (bh *BoltHelper) Set(btName string, key int64, value string) error {

	err := bh.db.Update(func(tx *bolt.Tx) error {
		buf := bytes.NewBuffer(make([]byte, 0))
		binary.Write(buf, binary.BigEndian, key)
		b := tx.Bucket([]byte(btName))
		if b == nil {
			bh.Logger.Error("[ERROR] Tablename[%v] not found", btName)
			return fmt.Errorf("Tablename[%v] not found", btName)
		}
		err := b.Put(buf.Bytes(), []byte(value))
		return err
	})

	return err
}

func (bh *BoltHelper) SetBatch(tablename string, kv map[int64]string) error {

	err := bh.db.Batch(func(tx *bolt.Tx) error {

		var s = make([]byte, 0)
		buf := bytes.NewBuffer(s)

		b := tx.Bucket([]byte(tablename))
		if b == nil {
			bh.Logger.Error("[ERROR] Tablename[%v] not found", tablename)
			return fmt.Errorf("Tablename[%v] not found", tablename)
		}
		for k, v := range kv {
			//fmt.Printf("k %v  v :%v ", k, v)
			binary.Write(buf, binary.BigEndian, k)
			if err := b.Put(buf.Bytes(), []byte(v)); err != nil {
				return err
			}
			buf.Reset()
		}
		return nil
	})

	return err

}

func (bh *BoltHelper) UpdateObj(tablename string, key int64, obj interface{}) error {

	value, err := json.Marshal(obj)
	if err != nil {
		bh.Logger.Error("%v", err)
		return err
	}
	var s = make([]byte, 0)
	buf := bytes.NewBuffer(s)
	binary.Write(buf, binary.BigEndian, key)

	err = bh.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tablename))
		if b == nil {
			bh.Logger.Error("[ERROR] Tablename[%v] not found", tablename)
			return fmt.Errorf("Tablename[%v] not found", tablename)
		}
		err := b.Put(buf.Bytes(), value)
		return err
	})

	return err
}

func (bh *BoltHelper) HasKey(btName string, key int64) bool {

	if _, err := bh.Get(btName, key); err != nil {
		return false
	}

	return true

}

func (bh *BoltHelper) GetNextKV(btName string, key int64) ([]byte, string, error) {

	var value []byte
	var bkey []byte
	bh.db.View(func(tx *bolt.Tx) error {

		var s = make([]byte, 0)
		buf := bytes.NewBuffer(s)
		binary.Write(buf, binary.BigEndian, key)

		b := tx.Bucket([]byte(btName)).Cursor()
		b.Seek(buf.Bytes())
		bkey, value = b.Next()
		//fmt.Printf("value : %v\n", string(value))
		//fmt.Printf("Key %v Next Key : %v  Value : %v\n", key, string(bkey), string(value))
		return nil
	})

	if value == nil || bkey == nil {
		//bh.Logger.Error("[ERROR] Key %v not found",key)
		return nil, "", fmt.Errorf("Key[%v] Not Found", key)
	}

	return bkey, string(value), nil

}

func (bh *BoltHelper) GetFirstKV(btName string) ([]byte, string, error) {

	var value []byte
	var key []byte

	bh.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(btName)).Cursor()
		key, value = b.First()
		fmt.Printf("First Key : %v  Value : %v\n", string(key), string(value))
		return nil
	})

	if value == nil {
		//bh.Logger.Error("[ERROR] Key %v not found",key)
		return nil, "", fmt.Errorf("Key[%v] Not Found", key)
	}

	return key, string(value), nil

}

func (bh *BoltHelper) Get(btName string, key int64) (string, error) {

	var value []byte
	var s = make([]byte, 0)
	buf := bytes.NewBuffer(s)
	binary.Write(buf, binary.BigEndian, key)
	bh.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(btName))
		value = b.Get(buf.Bytes())
		//fmt.Printf("value : %v\n", string(value))
		return nil
	})

	if value == nil {
		//bh.Logger.Error("[ERROR] Key %v not found",key)
		return "", fmt.Errorf("Key[%v] Not Found", key)
	}

	return string(value), nil
}

func (bh *BoltHelper) GetRange(btName string, keyMin int64, keyMax int64) ([]string, error) {

	var s1 = make([]byte, 0)
	min := bytes.NewBuffer(s1)
	binary.Write(min, binary.BigEndian, keyMin)

	var s2 = make([]byte, 0)
	max := bytes.NewBuffer(s2)
	binary.Write(max, binary.BigEndian, keyMax)
	res := make([]string, 0)

	bh.db.View(func(tx *bolt.Tx) error {

		c := tx.Bucket([]byte(btName)).Cursor()

		for k, v := c.Seek(min.Bytes()); k != nil && bytes.Compare(k, max.Bytes()) <= 0; k, v = c.Next() {
			res = append(res, string(v))
		}

		return nil
	})

	return res, nil
}

func (bh *BoltHelper) CloseDB() error {
	return bh.db.Close()
}

func (bh *BoltHelper) DisplayTable(tablename string) error {

	bh.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(tablename))

		var kv int64
		b.ForEach(func(k, v []byte) error {
			buf := bytes.NewBuffer(k)
			binary.Read(buf, binary.BigEndian, &kv)
			fmt.Printf("key=%v, value=%s\n", kv, v)
			return nil
		})
		return nil
	})

	return nil

}

func (bh *BoltHelper) Traverse(tablename string, tx *bolt.Tx) func() ([]byte, []byte) {

	var c *bolt.Cursor

	b := tx.Bucket([]byte(tablename))
	c = b.Cursor()

	k, v := c.First()
	return func() ([]byte, []byte) {

		if k != nil {
			k1, v1 := k, v
			k, v = c.Next()
			return k1, v1
		}

		return nil, nil

	}

}

func (bh *BoltHelper) BeginTx() (*bolt.Tx, error) {

	tx, err := bh.db.Begin(true)
	if err != nil {
		bh.Logger.Error("[ERROR] Create Tx Error %v ", err)
		return nil, err
	}
	return tx, nil

}

func (bh *BoltHelper) Commit(tx *bolt.Tx) error {

	if err := tx.Commit(); err != nil {
		bh.Logger.Error("[ERROR] Commit Tx Error %v", err)
		tx.Rollback()
		return err
	}

	return nil
}
