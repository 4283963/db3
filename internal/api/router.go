package api

import "github.com/gin-gonic/gin"

// NewRouter builds the Gin engine with middleware and routes registered.
func NewRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	r.GET("/health", h.Health)

	v1 := r.Group("/api/v1")
	{
		// POST with a JSON body is the canonical contract; GET is also
		// accepted (device_type taken from the query string) for the
		// convenience of ad-hoc curls and browser checks.
		v1.POST("/next-id", h.NextID)
		v1.GET("/next-id", h.NextID)
	}

	return r
}
