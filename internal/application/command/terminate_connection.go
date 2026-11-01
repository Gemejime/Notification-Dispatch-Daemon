// Package command implements application-layer command orchestrators for rag-notification-dispatch-daemon.
package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/gemejime/rag-notification-dispatch-daemon/internal/infrastructure/transport/hub"
)

// TerminateConnectionCommand carries input parameters for tearing down an SSE client connection.
type TerminateConnectionCommand struct {
	ConnectionID string
	TenantID     string
	UserID       string
}

// TerminateConnectionHandler orchestrates SSE session termination across the pod-local ConnectionHub
// and the distributed PresenceRegistrar directory.
type TerminateConnectionHandler struct {
	hub               *hub.ConnectionHub
	presenceRegistrar PresenceRegistrar
}

// NewTerminateConnectionHandler creates a new TerminateConnectionHandler.
func NewTerminateConnectionHandler(
	hub *hub.ConnectionHub,
	presenceRegistrar PresenceRegistrar,
) *TerminateConnectionHandler {
	return &TerminateConnectionHandler{
		hub:               hub,
		presenceRegistrar: presenceRegistrar,
	}
}

// Handle deregisters the connection from the local hub and removes presence from the directory.
func (h *TerminateConnectionHandler) Handle(ctx context.Context, cmd TerminateConnectionCommand) error {
	if h.hub == nil {
		return errors.New("connection hub cannot be nil")
	}

	conn, removed := h.hub.Deregister(cmd.ConnectionID)
	tenantID := cmd.TenantID
	userID := cmd.UserID
	if removed && conn != nil {
		if tenantID == "" {
			tenantID = conn.TenantID
		}
		if userID == "" {
			userID = conn.UserID
		}
	}

	if h.presenceRegistrar != nil && tenantID != "" {
		if err := h.presenceRegistrar.DeregisterPresence(ctx, tenantID, userID, cmd.ConnectionID); err != nil {
			return fmt.Errorf("deregister presence from directory: %w", err)
		}
	}

	return nil
}
