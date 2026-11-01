// Code generated from api/proto/v1/notification_event.proto and api/proto/v1/webhook_task.proto.
// Package notificationv1 provides Protobuf v3 contract types, binary/JSON wire marshalers,
// and schema validators for rag-notification-dispatch-daemon.
package notificationv1

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
)

// Severity represents the canonical notification severity level in notification_event.proto.
type Severity int32

const (
	Severity_SEVERITY_UNSPECIFIED Severity = 0
	Severity_SEVERITY_INFO        Severity = 1
	Severity_SEVERITY_WARNING     Severity = 2
	Severity_SEVERITY_ERROR       Severity = 3
	Severity_SEVERITY_CRITICAL    Severity = 4
)

// String returns the canonical string representation of Severity.
func (s Severity) String() string {
	switch s {
	case Severity_SEVERITY_INFO:
		return "INFO"
	case Severity_SEVERITY_WARNING:
		return "WARNING"
	case Severity_SEVERITY_ERROR:
		return "ERROR"
	case Severity_SEVERITY_CRITICAL:
		return "CRITICAL"
	default:
		return "UNSPECIFIED"
	}
}

// ParseSeverity converts a severity name or proto enum string into Severity.
func ParseSeverity(raw string) (Severity, error) {
	switch raw {
	case "INFO", "SEVERITY_INFO":
		return Severity_SEVERITY_INFO, nil
	case "WARNING", "SEVERITY_WARNING":
		return Severity_SEVERITY_WARNING, nil
	case "ERROR", "SEVERITY_ERROR":
		return Severity_SEVERITY_ERROR, nil
	case "CRITICAL", "SEVERITY_CRITICAL":
		return Severity_SEVERITY_CRITICAL, nil
	default:
		return Severity_SEVERITY_UNSPECIFIED, fmt.Errorf("invalid severity %q", raw)
	}
}

// NotificationEventEnvelope represents the Protobuf v3 CDC outbox event envelope
// defined in api/proto/v1/notification_event.proto.
type NotificationEventEnvelope struct {
	EventId          string   `json:"event_id"`
	TenantId         string   `json:"tenant_id"`
	UserId           string   `json:"user_id,omitempty"`
	SourceService    string   `json:"source_service"`
	EventType        string   `json:"event_type"`
	Severity         Severity `json:"severity"`
	IsTerminal       bool     `json:"is_terminal"`
	OccurredAtUnixMs int64    `json:"occurred_at_unix_ms"`
	TraceId          string   `json:"trace_id,omitempty"`
	PayloadJson      []byte   `json:"payload_json"`
}

// Validate checks all mandatory CDC outbox envelope invariants.
func (m *NotificationEventEnvelope) Validate() error {
	if m == nil {
		return errors.New("notification event envelope is nil")
	}
	if m.EventId == "" {
		return errors.New("event_id is required")
	}
	if m.TenantId == "" {
		return errors.New("tenant_id is required")
	}
	if m.SourceService == "" {
		return errors.New("source_service is required")
	}
	if m.EventType == "" {
		return errors.New("event_type is required")
	}
	if m.Severity < Severity_SEVERITY_INFO || m.Severity > Severity_SEVERITY_CRITICAL {
		return fmt.Errorf("invalid severity value: %d", m.Severity)
	}
	if m.OccurredAtUnixMs <= 0 {
		return errors.New("occurred_at_unix_ms must be positive")
	}
	if len(m.PayloadJson) == 0 || !json.Valid(m.PayloadJson) {
		return errors.New("payload_json must be valid non-empty JSON bytes")
	}
	return nil
}

// LogValue implements slog.LogValuer to enforce Secured Logging redaction of payload_json.
func (m NotificationEventEnvelope) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("event_id", m.EventId),
		slog.String("tenant_id", m.TenantId),
		slog.String("user_id", m.UserId),
		slog.String("source_service", m.SourceService),
		slog.String("event_type", m.EventType),
		slog.String("severity", m.Severity.String()),
		slog.Bool("is_terminal", m.IsTerminal),
		slog.Int64("occurred_at_unix_ms", m.OccurredAtUnixMs),
		slog.String("trace_id", m.TraceId),
		slog.Int("payload_bytes_len", len(m.PayloadJson)),
		slog.String("payload_json", "[REDACTED]"),
	)
}

// MarshalWire encodes NotificationEventEnvelope into deterministic Protobuf v3 binary wire format.
func (m *NotificationEventEnvelope) MarshalWire() ([]byte, error) {
	if m == nil {
		return nil, errors.New("cannot marshal nil NotificationEventEnvelope")
	}
	var buf []byte
	buf = appendProtoString(buf, 1, m.EventId)
	buf = appendProtoString(buf, 2, m.TenantId)
	buf = appendProtoString(buf, 3, m.UserId)
	buf = appendProtoString(buf, 4, m.SourceService)
	buf = appendProtoString(buf, 5, m.EventType)
	buf = appendProtoVarintField(buf, 6, uint64(m.Severity))
	if m.IsTerminal {
		buf = appendProtoVarintField(buf, 7, 1)
	}
	if m.OccurredAtUnixMs != 0 {
		buf = appendProtoVarintField(buf, 8, uint64(m.OccurredAtUnixMs))
	}
	buf = appendProtoString(buf, 9, m.TraceId)
	buf = appendProtoBytes(buf, 10, m.PayloadJson)
	return buf, nil
}

// UnmarshalWire decodes NotificationEventEnvelope from either Protobuf v3 binary wire format or JSON wire format.
func (m *NotificationEventEnvelope) UnmarshalWire(data []byte) error {
	if len(data) == 0 {
		return errors.New("empty wire payload")
	}
	// Support JSON wire representation transparently if payload starts with '{'.
	if data[0] == '{' {
		type jsonAlias struct {
			EventId          string          `json:"event_id"`
			TenantId         string          `json:"tenant_id"`
			UserId           string          `json:"user_id"`
			SourceService    string          `json:"source_service"`
			EventType        string          `json:"event_type"`
			Severity         any             `json:"severity"`
			IsTerminal       bool            `json:"is_terminal"`
			OccurredAtUnixMs int64           `json:"occurred_at_unix_ms"`
			TraceId          string          `json:"trace_id"`
			PayloadJson      json.RawMessage `json:"payload_json"`
		}
		var aux jsonAlias
		if err := json.Unmarshal(data, &aux); err != nil {
			return fmt.Errorf("unmarshal json envelope: %w", err)
		}
		m.EventId = aux.EventId
		m.TenantId = aux.TenantId
		m.UserId = aux.UserId
		m.SourceService = aux.SourceService
		m.EventType = aux.EventType
		m.IsTerminal = aux.IsTerminal
		m.OccurredAtUnixMs = aux.OccurredAtUnixMs
		m.TraceId = aux.TraceId
		switch v := aux.Severity.(type) {
		case float64:
			m.Severity = Severity(int32(v))
		case string:
			sev, err := ParseSeverity(v)
			if err != nil {
				return err
			}
			m.Severity = sev
		}
		if len(aux.PayloadJson) > 0 {
			if aux.PayloadJson[0] == '"' {
				var decoded []byte
				if err := json.Unmarshal(aux.PayloadJson, &decoded); err != nil {
					return fmt.Errorf("decode base64 payload_json: %w", err)
				}
				m.PayloadJson = decoded
			} else {
				m.PayloadJson = append(m.PayloadJson[:0], aux.PayloadJson...)
			}
		}
		return nil
	}

	*m = NotificationEventEnvelope{}
	idx := 0
	for idx < len(data) {
		tag, n := binary.Uvarint(data[idx:])
		if n <= 0 {
			return errors.New("malformed protobuf varint tag")
		}
		idx += n
		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch wireType {
		case 0: // Varint
			val, vn := binary.Uvarint(data[idx:])
			if vn <= 0 {
				return errors.New("malformed protobuf varint value")
			}
			idx += vn
			switch fieldNum {
			case 6:
				m.Severity = Severity(int32(val))
			case 7:
				m.IsTerminal = val != 0
			case 8:
				m.OccurredAtUnixMs = int64(val)
			}
		case 2: // Length-delimited
			length, ln := binary.Uvarint(data[idx:])
			if ln <= 0 || idx+ln+int(length) > len(data) {
				return errors.New("malformed protobuf length-delimited field")
			}
			idx += ln
			fieldBytes := data[idx : idx+int(length)]
			idx += int(length)
			switch fieldNum {
			case 1:
				m.EventId = string(fieldBytes)
			case 2:
				m.TenantId = string(fieldBytes)
			case 3:
				m.UserId = string(fieldBytes)
			case 4:
				m.SourceService = string(fieldBytes)
			case 5:
				m.EventType = string(fieldBytes)
			case 9:
				m.TraceId = string(fieldBytes)
			case 10:
				m.PayloadJson = append(m.PayloadJson[:0], fieldBytes...)
			}
		default:
			return fmt.Errorf("unsupported protobuf wire type %d", wireType)
		}
	}
	return nil
}

// WebhookDeliveryTask represents the Protobuf v3 downstream webhook task schema
// defined in api/proto/v1/webhook_task.proto.
type WebhookDeliveryTask struct {
	TaskId              string `json:"task_id"`
	EventId             string `json:"event_id"`
	TenantId            string `json:"tenant_id"`
	TargetId            string `json:"target_id"`
	TargetUrl           string `json:"target_url"`
	SecretRef           string `json:"secret_ref"`
	SignatureHeaderName string `json:"signature_header_name"`
	TimestampUnixSec    int64  `json:"timestamp_unix_sec"`
	EventType           string `json:"event_type"`
	PayloadJson         []byte `json:"payload_json"`
	TraceId             string `json:"trace_id,omitempty"`
}

// Validate checks all mandatory WebhookDeliveryTask invariants.
func (t *WebhookDeliveryTask) Validate() error {
	if t == nil {
		return errors.New("webhook delivery task is nil")
	}
	if t.TaskId == "" {
		return errors.New("task_id is required")
	}
	if t.EventId == "" {
		return errors.New("event_id is required")
	}
	if t.TenantId == "" {
		return errors.New("tenant_id is required")
	}
	if t.TargetId == "" {
		return errors.New("target_id is required")
	}
	if t.TargetUrl == "" {
		return errors.New("target_url is required")
	}
	if t.SecretRef == "" {
		return errors.New("secret_ref is required")
	}
	if t.SignatureHeaderName == "" {
		return errors.New("signature_header_name is required")
	}
	if t.TimestampUnixSec <= 0 {
		return errors.New("timestamp_unix_sec must be positive")
	}
	if t.EventType == "" {
		return errors.New("event_type is required")
	}
	if len(t.PayloadJson) == 0 || !json.Valid(t.PayloadJson) {
		return errors.New("payload_json must be valid non-empty JSON bytes")
	}
	return nil
}

// LogValue implements slog.LogValuer to enforce Secured Logging redaction of secret_ref and payload_json.
func (t WebhookDeliveryTask) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("task_id", t.TaskId),
		slog.String("event_id", t.EventId),
		slog.String("tenant_id", t.TenantId),
		slog.String("target_id", t.TargetId),
		slog.String("target_url", t.TargetUrl),
		slog.String("secret_ref", "[REDACTED]"),
		slog.String("signature_header_name", t.SignatureHeaderName),
		slog.Int64("timestamp_unix_sec", t.TimestampUnixSec),
		slog.String("event_type", t.EventType),
		slog.Int("payload_bytes_len", len(t.PayloadJson)),
		slog.String("payload_json", "[REDACTED]"),
		slog.String("trace_id", t.TraceId),
	)
}

// MarshalWire encodes WebhookDeliveryTask into deterministic Protobuf v3 binary wire format.
func (t *WebhookDeliveryTask) MarshalWire() ([]byte, error) {
	if t == nil {
		return nil, errors.New("cannot marshal nil WebhookDeliveryTask")
	}
	var buf []byte
	buf = appendProtoString(buf, 1, t.TaskId)
	buf = appendProtoString(buf, 2, t.EventId)
	buf = appendProtoString(buf, 3, t.TenantId)
	buf = appendProtoString(buf, 4, t.TargetId)
	buf = appendProtoString(buf, 5, t.TargetUrl)
	buf = appendProtoString(buf, 6, t.SecretRef)
	buf = appendProtoString(buf, 7, t.SignatureHeaderName)
	if t.TimestampUnixSec != 0 {
		buf = appendProtoVarintField(buf, 8, uint64(t.TimestampUnixSec))
	}
	buf = appendProtoString(buf, 9, t.EventType)
	buf = appendProtoBytes(buf, 10, t.PayloadJson)
	buf = appendProtoString(buf, 11, t.TraceId)
	return buf, nil
}

// UnmarshalWire decodes WebhookDeliveryTask from Protobuf v3 binary wire format.
func (t *WebhookDeliveryTask) UnmarshalWire(data []byte) error {
	if len(data) == 0 {
		return errors.New("empty wire payload")
	}
	*t = WebhookDeliveryTask{}
	idx := 0
	for idx < len(data) {
		tag, n := binary.Uvarint(data[idx:])
		if n <= 0 {
			return errors.New("malformed protobuf varint tag")
		}
		idx += n
		fieldNum := tag >> 3
		wireType := tag & 0x7

		switch wireType {
		case 0:
			val, vn := binary.Uvarint(data[idx:])
			if vn <= 0 {
				return errors.New("malformed protobuf varint value")
			}
			idx += vn
			if fieldNum == 8 {
				t.TimestampUnixSec = int64(val)
			}
		case 2:
			length, ln := binary.Uvarint(data[idx:])
			if ln <= 0 || idx+ln+int(length) > len(data) {
				return errors.New("malformed protobuf length-delimited field")
			}
			idx += ln
			fieldBytes := data[idx : idx+int(length)]
			idx += int(length)
			switch fieldNum {
			case 1:
				t.TaskId = string(fieldBytes)
			case 2:
				t.EventId = string(fieldBytes)
			case 3:
				t.TenantId = string(fieldBytes)
			case 4:
				t.TargetId = string(fieldBytes)
			case 5:
				t.TargetUrl = string(fieldBytes)
			case 6:
				t.SecretRef = string(fieldBytes)
			case 7:
				t.SignatureHeaderName = string(fieldBytes)
			case 9:
				t.EventType = string(fieldBytes)
			case 10:
				t.PayloadJson = append(t.PayloadJson[:0], fieldBytes...)
			case 11:
				t.TraceId = string(fieldBytes)
			}
		default:
			return fmt.Errorf("unsupported protobuf wire type %d", wireType)
		}
	}
	return nil
}

func appendProtoVarintField(buf []byte, fieldNum uint64, val uint64) []byte {
	tag := (fieldNum << 3) | 0
	buf = binary.AppendUvarint(buf, tag)
	buf = binary.AppendUvarint(buf, val)
	return buf
}

func appendProtoString(buf []byte, fieldNum uint64, s string) []byte {
	if s == "" {
		return buf
	}
	tag := (fieldNum << 3) | 2
	buf = binary.AppendUvarint(buf, tag)
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	buf = append(buf, s...)
	return buf
}

func appendProtoBytes(buf []byte, fieldNum uint64, b []byte) []byte {
	if len(b) == 0 {
		return buf
	}
	tag := (fieldNum << 3) | 2
	buf = binary.AppendUvarint(buf, tag)
	buf = binary.AppendUvarint(buf, uint64(len(b)))
	buf = append(buf, b...)
	return buf
}
