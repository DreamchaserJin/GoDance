/**
 * @Author itcast
 * @Date 5:46 AM 5/22/22
 * @Note
 **/

package test

import (
	"fmt"
	"gdindex/tree"
	"github.com/boltdb/bolt"
	"log"
	"testing"
	"utils"
)

func TestBoltDB(t *testing.T) {
	db, err := bolt.Open("./test.bt", 0644, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("id"))
		if b != nil {
			get := b.Get([]byte("羽毛球"))
			fmt.Println(string(get))
		}

		return nil
	})
}

func TestCreateDB(t *testing.T) {
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("err happen: %v", err)
	}

	btdb := tree.NewBTDB("./test.bt", logger)

	err = btdb.AddTree("http")
	if err != nil {
		return
	}

	btdb.Set("http", "name", uint64(124))
	btdb.Set("http", "maoqiu", uint64(12))
	btdb.Set("http", "mmp", uint64(552))

	search, u := btdb.Search("http", "name")
	if search {
		fmt.Println(u)
	}
}

func TestInt(t *testing.T) {
	fmt.Println(getInt())
}

func getInt() int32 {
	return -1
}
