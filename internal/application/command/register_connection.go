// Package command implements application-layer command orchestrators for rag-notification-dispatch-daemon.
package command

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/domain/entity"
	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
)

// PresenceRegistrar defines the outbound port for registering/deregistering client socket
// presence in the distributed directory (Redis PresenceRegistry in Phase 3, ADR-003).
type PresenceRegistrar interface {
	RegisterPresence(ctx context.Context, tenantID, userID, nodeID, connectionID string, ttl time.Duration) error
	DeregisterPresence(ctx context.Context, tenantID, userID, connectionID string) error
	RefreshPresence(ctx context.Context, tenantID, userID, connectionID string, ttl time.Duration) error
}

// RegisterConnectionCommand carries input parameters for registering a new SSE client connection.
type RegisterConnectionCommand struct {
	Conn       entity.Connection
	RingBuffer *hub.RingBuffer
	Cancel     context.CancelFunc
}

// RegisterConnectionHandler orchestrates SSE session registration in the pod-local ConnectionHub
// and the distributed PresenceRegistrar directory.
type RegisterConnectionHandler struct {
	hub               *hub.ConnectionHub
	presenceRegistrar PresenceRegistrar
	presenceTTL       time.Duration
}

// NewRegisterConnectionHandler creates a new RegisterConnectionHandler.
func NewRegisterConnectionHandler(
	hub *hub.ConnectionHub,
	presenceRegistrar PresenceRegistrar,
	presenceTTL time.Duration,
) *RegisterConnectionHandler {
	if presenceTTL <= 0 {
		presenceTTL = 45 * time.Second
	}
	return &RegisterConnectionHandler{
		hub:               hub,
		presenceRegistrar: presenceRegistrar,
		presenceTTL:       presenceTTL,
	}
}

// Handle validates and registers the connection locally and in the presence registry.
func (h *RegisterConnectionHandler) Handle(ctx context.Context, cmd RegisterConnectionCommand) error {
	if h.hub == nil {
		return errors.New("connection hub cannot be nil")
	}

	if err := h.hub.Register(cmd.Conn, cmd.RingBuffer, cmd.Cancel); err != nil {
		return fmt.Errorf("register connection in hub: %w", err)
	}

	if h.presenceRegistrar != nil {
		if err := h.presenceRegistrar.RegisterPresence(
			ctx,
			cmd.Conn.TenantID,
			cmd.Conn.UserID,
			cmd.Conn.NodeID,
			cmd.Conn.ConnectionID,
			h.presenceTTL,
		); err != nil {
			// Clean up local hub on presence registration failure
			h.hub.Deregister(cmd.Conn.ConnectionID)
			return fmt.Errorf("register presence in directory: %w", err)
		}
	}

	return nil
}
