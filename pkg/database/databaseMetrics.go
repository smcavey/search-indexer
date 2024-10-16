package database

import (
	"context"
	"k8s.io/klog/v2"
)

func (dao *DAO) GetDatabaseSize(ctx context.Context) float64 {
	var dbSize int64
	existingRows, err := dao.pool.Query(ctx,
		"SELECT CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT') THEN pg_catalog.pg_database_size(d.datname) ELSE 0 END AS db_size FROM pg_catalog.pg_database d WHERE d.datname = 'search'",
	)
	if err != nil {
		klog.Warningf("Error getting size of database. Error: %+v", err)
	}
	for existingRows.Next() {
		if err = existingRows.Scan(&dbSize); err != nil {
			klog.Warningf("Error unmarshalling existing resource row. Error: %+v", err)
		}
	}
	existingRows.Close()
	return float64(dbSize)
}
