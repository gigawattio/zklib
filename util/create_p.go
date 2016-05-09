package util

import (
	"fmt"
	"strings"

	"gigawatt-common/pkg/gentle"

	"github.com/cenkalti/backoff"
	"github.com/samuel/go-zookeeper/zk"
)

// CreateP functions similarly to `mkdir -p`.
func CreateP(conn *zk.Conn, path string, data []byte, flags int32, acl []zk.ACL) (zxIds []string, err error) {
	zxIds = []string{}
	pieces := strings.Split(strings.Trim(path, "/"), "/")
	var (
		zxId  string
		soFar string
	)
	for _, piece := range pieces {
		soFar += "/" + piece
		if zxId, err = conn.Create(soFar, data, flags, acl); err != nil && err != zk.ErrNodeExists {
			return
		}
		zxIds = append(zxIds, zxId)
	}
	err = nil // Clear out any potential error state, since if we made it this far we're OK.
	return
}

// MustCreateP will keep trying to create the path until it succeeds.
func MustCreateP(conn *zk.Conn, path string, data []byte, flags int32, acl []zk.ACL, strategy backoff.BackOff) (zxIds []string) {
	var err error
	operation := func() error {
		if zxIds, err = CreateP(conn, path, []byte{}, 0, acl); err != nil {
			return err
		}
		return nil
	}
	gentle.RetryUntilSuccess(fmt.Sprintf("conn=%p MustCreateP", conn), operation, strategy)
	return
}

func MustCreateProtectedEphemeralSequential(conn *zk.Conn, path string, data []byte, acl []zk.ACL, strategy backoff.BackOff) (zxId string) {
	var err error
	operation := func() error {
		if pieces := strings.Split(path, "/"); len(pieces) > 2 {
			basePath := strings.Join(pieces[0:len(pieces)-1], "/")
			if _, err = CreateP(conn, basePath, []byte{}, 0, acl); err != nil {
				return err
			}
		}
		if zxId, err = conn.CreateProtectedEphemeralSequential(path, data, acl); err != nil {
			return err
		}
		return nil
	}
	gentle.RetryUntilSuccess(fmt.Sprintf("MustCreateProtectedEphemeralSequential conn=%p path=%v", conn, path), operation, strategy)
	return
}
