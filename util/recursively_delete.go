package util

import (
	"time"

	"github.com/gigawattio/concurrency"

	log "github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
)

func RecursivelyDelete(conn *zk.Conn, path string, numRetries ...int) error {
	var wipe func(conn *zk.Conn, path string) error

	wipe = func(conn *zk.Conn, path string) error {
		exists, stat, err := conn.Exists(path)
		if err != nil && err != zk.ErrNoNode {
			return err
		}
		if exists {
			// Wipe out any and all children.
			var children []string
			if children, _, err = conn.Children(path); err != nil && err != zk.ErrNoNode {
				return err
			}
			deleterFuncs := make([]func() error, 0, len(children))
			for _, child := range children {
				func(child string) {
					deleterFunc := func() (err error) {
						attempt := 0
					Retry:
						if err = wipe(conn, path+"/"+child); err != nil {
							if (err == zk.ErrConnectionClosed || err == zk.ErrNotEmpty) && len(numRetries) > 0 && numRetries[0] > attempt {
								attempt++
								log.Infof("Retrying failed deletion attempt #%v", attempt)
								time.Sleep(1 * time.Millisecond)
								goto Retry
							}
							return
						}
						return
					}
					deleterFuncs = append(deleterFuncs, deleterFunc)
				}(child)
			}
			if err = concurrency.MultiGo(deleterFuncs...); err != nil {
				return err
			}
			if err = conn.Delete(path, stat.Version); err != nil && err != zk.ErrNoNode {
				return err
			}
		}
		return nil
	}

	if err := wipe(conn, path); err != nil {
		return err
	}
	return nil
}
