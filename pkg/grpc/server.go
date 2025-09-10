// Copyright Contributors to the Open Cluster Management project

package grpc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/stolostron/search-indexer/api/proto"
	"github.com/stolostron/search-indexer/pkg/config"
	"github.com/stolostron/search-indexer/pkg/database"
	"github.com/stolostron/search-indexer/pkg/metrics"
	"github.com/stolostron/search-indexer/pkg/model"
	"k8s.io/klog/v2"
)

type SearchIndexerService struct {
	proto.UnimplementedSearchIndexerServer
	Dao *database.DAO
}

func NewSearchIndexerService(dao *database.DAO) *SearchIndexerService {
	return &SearchIndexerService{
		Dao: dao,
	}
}

func (s *SearchIndexerService) Sync(ctx context.Context, req *proto.SyncRequest) (*proto.SyncResponse, error) {
	start := time.Now()
	clusterName := req.GetClusterId()

	// Convert gRPC request to internal model
	syncEvent := protoToSyncEvent(req)

	resourceTotal := len(syncEvent.AddResources) + len(syncEvent.UpdateResources) + len(syncEvent.DeleteResources)
	metrics.RequestSize.Observe(float64(resourceTotal))

	// Initialize SyncResponse object
	syncResponse := &model.SyncResponse{
		Version:          config.COMPONENT_VERSION,
		AddErrors:        make([]model.SyncError, 0),
		UpdateErrors:     make([]model.SyncError, 0),
		DeleteErrors:     make([]model.SyncError, 0),
		AddEdgeErrors:    make([]model.SyncError, 0),
		DeleteEdgeErrors: make([]model.SyncError, 0),
	}

	var err error
	if req.GetOverwriteState() {
		// For resync requests, we need to serialize the request to bytes
		bodyBytes, marshalErr := json.Marshal(syncEvent)
		if marshalErr != nil {
			klog.Errorf("Error marshaling sync event for cluster [%s]. Error: %+v\n", clusterName, marshalErr)
			return nil, marshalErr
		}
		err = s.Dao.ResyncData(ctx, clusterName, syncResponse, bodyBytes)
	} else {
		err = s.Dao.SyncData(ctx, syncEvent, clusterName, syncResponse)
	}

	if err != nil {
		klog.Warningf("Error processing gRPC sync request from cluster %s. Error: %s", clusterName, err)
		return nil, err
	}

	// Get the total cluster resources for validation by the collector
	totalResources, totalEdges, validateErr := s.Dao.ClusterTotals(ctx, clusterName)
	if validateErr != nil {
		klog.Warningf("Error getting cluster totals for cluster %s. Error: %s", clusterName, validateErr)
		return nil, validateErr
	}
	syncResponse.TotalResources = totalResources
	syncResponse.TotalEdges = totalEdges

	// Convert internal response to gRPC response
	grpcResponse := syncResponseToProto(syncResponse)

	// Log request
	klog.V(5).Infof("gRPC request from [%12s] took [%v] overwriteState [%t] addTotal [%d]",
		clusterName, time.Since(start), req.GetOverwriteState(), len(syncEvent.AddResources))

	return grpcResponse, nil
}

func (s *SearchIndexerService) Health(ctx context.Context, req *proto.HealthRequest) (*proto.HealthResponse, error) {
	return &proto.HealthResponse{
		Status: "OK",
	}, nil
}

// Helper functions to convert between gRPC and internal models

func protoToSyncEvent(req *proto.SyncRequest) model.SyncEvent {
	syncEvent := model.SyncEvent{
		AddResources:    make([]model.Resource, len(req.AddResources)),
		UpdateResources: make([]model.Resource, len(req.UpdateResources)),
		DeleteResources: make([]model.DeleteResourceEvent, len(req.DeleteResources)),
		AddEdges:        make([]model.Edge, len(req.AddEdges)),
		DeleteEdges:     make([]model.Edge, len(req.DeleteEdges)),
	}

	// Convert AddResources
	for i, protoRes := range req.AddResources {
		syncEvent.AddResources[i] = protoToResource(protoRes)
	}

	// Convert UpdateResources
	for i, protoRes := range req.UpdateResources {
		syncEvent.UpdateResources[i] = protoToResource(protoRes)
	}

	// Convert DeleteResources
	for i, protoDel := range req.DeleteResources {
		syncEvent.DeleteResources[i] = model.DeleteResourceEvent{
			UID: protoDel.GetUid(),
		}
	}

	// Convert AddEdges
	for i, protoEdge := range req.AddEdges {
		syncEvent.AddEdges[i] = protoToEdge(protoEdge)
	}

	// Convert DeleteEdges
	for i, protoEdge := range req.DeleteEdges {
		syncEvent.DeleteEdges[i] = protoToEdge(protoEdge)
	}

	return syncEvent
}

func protoToResource(protoRes *proto.Resource) model.Resource {
	// Convert map[string]string to map[string]interface{}
	properties := make(map[string]interface{})
	for k, v := range protoRes.GetProperties() {
		properties[k] = v
	}

	return model.Resource{
		Kind:           protoRes.GetKind(),
		UID:            protoRes.GetUid(),
		ResourceString: protoRes.GetResourceString(),
		Properties:     properties,
	}
}

func protoToEdge(protoEdge *proto.Edge) model.Edge {
	return model.Edge{
		SourceUID:  protoEdge.GetSourceUid(),
		DestUID:    protoEdge.GetDestUid(),
		EdgeType:   protoEdge.GetEdgeType(),
		SourceKind: protoEdge.GetSourceKind(),
		DestKind:   protoEdge.GetDestKind(),
	}
}

func syncResponseToProto(resp *model.SyncResponse) *proto.SyncResponse {
	grpcResp := &proto.SyncResponse{
		TotalAdded:        int32(resp.TotalAdded),
		TotalUpdated:      int32(resp.TotalUpdated),
		TotalDeleted:      int32(resp.TotalDeleted),
		TotalResources:    int32(resp.TotalResources),
		TotalEdgesAdded:   int32(resp.TotalEdgesAdded),
		TotalEdgesDeleted: int32(resp.TotalEdgesDeleted),
		TotalEdges:        int32(resp.TotalEdges),
		Version:           resp.Version,
		AddErrors:         make([]*proto.SyncError, len(resp.AddErrors)),
		UpdateErrors:      make([]*proto.SyncError, len(resp.UpdateErrors)),
		DeleteErrors:      make([]*proto.SyncError, len(resp.DeleteErrors)),
		AddEdgeErrors:     make([]*proto.SyncError, len(resp.AddEdgeErrors)),
		DeleteEdgeErrors:  make([]*proto.SyncError, len(resp.DeleteEdgeErrors)),
	}

	// Convert errors
	for i, err := range resp.AddErrors {
		grpcResp.AddErrors[i] = &proto.SyncError{
			ResourceUid: err.ResourceUID,
			Message:     err.Message,
		}
	}

	for i, err := range resp.UpdateErrors {
		grpcResp.UpdateErrors[i] = &proto.SyncError{
			ResourceUid: err.ResourceUID,
			Message:     err.Message,
		}
	}

	for i, err := range resp.DeleteErrors {
		grpcResp.DeleteErrors[i] = &proto.SyncError{
			ResourceUid: err.ResourceUID,
			Message:     err.Message,
		}
	}

	for i, err := range resp.AddEdgeErrors {
		grpcResp.AddEdgeErrors[i] = &proto.SyncError{
			ResourceUid: err.ResourceUID,
			Message:     err.Message,
		}
	}

	for i, err := range resp.DeleteEdgeErrors {
		grpcResp.DeleteEdgeErrors[i] = &proto.SyncError{
			ResourceUid: err.ResourceUID,
			Message:     err.Message,
		}
	}

	return grpcResp
}
