package remote

import (
	"crypto/md5"
	"encoding/hex"
	"net"
	"net/http"
	"net/rpc"
)

// 加密工具类
type EncryptionUtil struct {
}

// 加密方法
func (eu *EncryptionUtil) Encryption(req string, resp *string) error {
	*resp = ToMd5(req)
	return nil
}

// 封装 md5 方法
func ToMd5(s string) string {
	m := md5.New()
	m.Write([]byte(s))
	return hex.EncodeToString(m.Sum(nil))
}

func init() {
	// 功能对象注册
	encryption := new(EncryptionUtil)
	err := rpc.Register(encryption) //rpc.RegisterName("自定义服务名",encryption)
	if err != nil {
		panic(err.Error())
	}
	// HTTP注册
	rpc.HandleHTTP()

	// 端口监听
	listen, err := net.Listen("tcp", ":8081")
	if err != nil {
		panic(err.Error())
	}
	// 启动服务
	_ = http.Serve(listen, nil)

}
