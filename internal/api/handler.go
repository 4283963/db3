// Package api implements the HTTP layer: request/response DTOs, the
// /api/v1/next-id handler and the router wiring.
package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/kl/db3/internal/idgen"
)

// NextIDRequest is the JSON body accepted by the endpoint.
type NextIDRequest struct {
	DeviceType string `json:"device_type"`
	Region     string `json:"region"`
}

// NextIDData is the payload returned on success.
type NextIDData struct {
	// ID is the 16-character serial number. The first 2 characters are the
	// upper-case region code (or "XX" by default), the remaining 14 are
	// digits from the snowflake generator.
	ID string `json:"id"`
	// Region echoes the region code used (after normalisation to upper case).
	Region string `json:"region"`
	// DeviceType echoes the device type the id was generated for.
	DeviceType string `json:"device_type"`
	// IntValue is the raw 64-bit id (informational; the canonical form is ID).
	IntValue int64 `json:"int_value"`
	// GeneratedAt is the wall-clock time of generation (RFC3339).
	GeneratedAt string `json:"generated_at"`
}

// Response is the uniform envelope for every API reply.
type Response struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data,omitempty"`
}

// Handler holds the dependencies required to serve requests.
type Handler struct {
	mgr *idgen.GeneratorManager
}

// NewHandler builds a handler around the given generator manager.
func NewHandler(mgr *idgen.GeneratorManager) *Handler {
	return &Handler{mgr: mgr}
}

// NextID handles GET and POST /api/v1/next-id. The device type and region
// are read from query parameters first, and fall back to a JSON body so the
// endpoint works for both callers and curl one-liners.
//
// When no region is supplied it defaults to "XX". The region must be exactly
// 2 ASCII letters (case-insensitive) and is returned in upper case.
func (h *Handler) NextID(c *gin.Context) {
	deviceType := strings.TrimSpace(c.Query("device_type"))
	region := strings.TrimSpace(c.Query("region"))
	if deviceType == "" {
		var req NextIDRequest
		if err := c.ShouldBindJSON(&req); err == nil {
			deviceType = strings.TrimSpace(req.DeviceType)
			if region == "" {
				region = strings.TrimSpace(req.Region)
			}
		}
	}

	if deviceType == "" {
		c.JSON(http.StatusBadRequest, Response{Code: 400, Msg: "device_type is required"})
		return
	}
	if len(deviceType) > 64 {
		c.JSON(http.StatusBadRequest, Response{Code: 400, Msg: "device_type too long (max 64 chars)"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	id, err := h.mgr.NextID(ctx, deviceType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code: 500,
			Msg:  "generate id failed: " + err.Error(),
		})
		return
	}

	formatted, err := idgen.FormatIDWithRegion(id, region)
	if err != nil {
		c.JSON(http.StatusBadRequest, Response{Code: 400, Msg: err.Error()})
		return
	}

	normRegion := formatted[:idgen.RegionLength]

	c.JSON(http.StatusOK, Response{
		Code: 0,
		Msg:  "ok",
		Data: NextIDData{
			ID:          formatted,
			Region:      normRegion,
			DeviceType:  deviceType,
			IntValue:    id,
			GeneratedAt: time.Now().Format(time.RFC3339),
		},
	})
}

// Health is a liveness probe.
func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
