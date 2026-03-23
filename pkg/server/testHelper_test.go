// Copyright Contributors to the Open Cluster Management project
package server

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stolostron/search-indexer/pkg/database"
	"github.com/stolostron/search-indexer/pkg/testutils"
)

// Builds a ServerConfig instance with a mock database connection.
func buildMockServer(t *testing.T) (ServerConfig, *testutils.MockPgxPool) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPool := testutils.NewMockPgxPool(ctrl)

	dao := database.NewDAO(mockPool)
	server := ServerConfig{
		Dao: &dao,
	}
	return server, mockPool
}
