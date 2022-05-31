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

	btdb := tree.NewBTDB("./test.bt", nil)

	err := btdb.AddBTree("http")
	if err != nil {
		return
	}

}

func TestInt(t *testing.T) {
	fmt.Println(getInt())
}

func getInt() int32 {
	return -1
}
