package util

import (
	"github.com/samuel/go-zookeeper/zk"
)

func RecursivelyDelete(conn *zk.Conn, path string) error {
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
			for _, child := range children {
				if err = wipe(conn, path+"/"+child); err != nil {
					return err
				}
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
