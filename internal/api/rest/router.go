// Package rest provides the Gin-based REST API server.
package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"

	pbm "github.com/iggydv12/gomad/gen/proto/models"
	"github.com/iggydv12/gomad/internal/ledger"
	"github.com/iggydv12/gomad/internal/storage"
)

// Server is the REST API server.
type Server struct {
	engine  *gin.Engine
	storage *storage.PeerStorage
	ledger  *ledger.GroupLedger
	logger  *zap.Logger
}

// New creates a REST Server.
func New(ps *storage.PeerStorage, gl *ledger.GroupLedger, logger *zap.Logger) *Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	s := &Server{
		engine:  engine,
		storage: ps,
		ledger:  gl,
		logger:  logger,
	}
	s.registerRoutes()
	return s
}

// Start starts the REST server on addr.
func (s *Server) Start(addr string) error {
	s.logger.Info("REST API listening", zap.String("addr", addr))
	return s.engine.Run(addr)
}

// registerRoutes sets up the /nomad context path.
func (s *Server) registerRoutes() {
	nomad := s.engine.Group("/nomad")

	// Swagger UI
	nomad.GET("/swagger-ui/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// Local storage endpoints
	storageGroup := nomad.Group("/storage")
	{
		storageGroup.POST("/put", s.put)
		storageGroup.GET("/get/:id", s.get)
		storageGroup.PUT("/update", s.update)
		storageGroup.DELETE("/delete/:id", s.delete)
		storageGroup.GET("/get/peer-ledger", s.getPeerLedger)
		storageGroup.GET("/get/object-ledger", s.getObjectLedger)
	}

	// Group storage endpoints
	groupGroup := nomad.Group("/group-storage")
	{
		groupGroup.POST("/fast-put", s.fastPut)
		groupGroup.POST("/safe-put", s.safePut)
		groupGroup.GET("/fast-get/:id", s.fastGet)
		groupGroup.GET("/parallel-get/:id", s.parallelGet)
		groupGroup.GET("/safe-get/:id", s.safeGet)
	}

	// Overlay storage endpoints
	overlayGroup := nomad.Group("/overlay-storage")
	{
		overlayGroup.POST("/put", s.overlayPut)
		overlayGroup.GET("/get/:id", s.overlayGet)
	}

	// Config endpoints
	configGroup := nomad.Group("/config")
	{
		configGroup.POST("/storage", s.setStorageMode)
		configGroup.POST("/retrieval", s.setRetrievalMode)
		configGroup.POST("/groups/migration", s.setMigration)
		configGroup.POST("/groups/replication-factor", s.setRF)
	}
}

// --- Storage handlers ---

// @Summary Store a game object
// @Tags storage
// @Accept json
// @Produce json
// @Param object body pbm.GameObjectGrpc true "Game object"
// @Success 200 {object} map[string]bool
// @Router /nomad/storage/put [post]
func (s *Server) put(c *gin.Context) {
	var obj pbm.GameObjectGrpc
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ok, err := s.storage.Put(&obj, true, true)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": ok})
}

func (s *Server) get(c *gin.Context) {
	id := c.Param("id")
	obj, err := s.storage.Get(id, true, true)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, obj)
}

func (s *Server) update(c *gin.Context) {
	var obj pbm.GameObjectGrpc
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ok, err := s.storage.Update(&obj)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": ok})
}

func (s *Server) delete(c *gin.Context) {
	id := c.Param("id")
	ok, err := s.storage.Delete(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": ok})
}

func (s *Server) getPeerLedger(c *gin.Context) {
	c.JSON(http.StatusOK, s.ledger.GetPeerLedgerSnapshot())
}

func (s *Server) getObjectLedger(c *gin.Context) {
	c.JSON(http.StatusOK, s.ledger.GetObjectLedgerSnapshot())
}

// --- Group storage handlers ---

func (s *Server) fastPut(c *gin.Context) {
	var obj pbm.GameObjectGrpc
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ok, err := s.storage.Put(&obj, true, false)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": ok})
}

func (s *Server) safePut(c *gin.Context) {
	var obj pbm.GameObjectGrpc
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s.storage.SetStorageMode("safe")
	ok, err := s.storage.Put(&obj, true, false)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": ok})
}

func (s *Server) fastGet(c *gin.Context) {
	id := c.Param("id")
	s.storage.SetRetrievalMode("fast")
	obj, err := s.storage.Get(id, true, false)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, obj)
}

func (s *Server) parallelGet(c *gin.Context) {
	id := c.Param("id")
	s.storage.SetRetrievalMode("parallel")
	obj, err := s.storage.Get(id, true, false)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, obj)
}

func (s *Server) safeGet(c *gin.Context) {
	id := c.Param("id")
	s.storage.SetRetrievalMode("safe")
	obj, err := s.storage.Get(id, true, false)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, obj)
}

// --- Overlay handlers ---

func (s *Server) overlayPut(c *gin.Context) {
	var obj pbm.GameObjectGrpc
	if err := c.ShouldBindJSON(&obj); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ok, err := s.storage.Put(&obj, false, true)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": ok})
}

func (s *Server) overlayGet(c *gin.Context) {
	id := c.Param("id")
	obj, err := s.storage.Get(id, false, true)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, obj)
}

// --- Config handlers ---

func (s *Server) setStorageMode(c *gin.Context) {
	var body struct{ Mode string `json:"mode"` }
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s.storage.SetStorageMode(body.Mode)
	c.JSON(http.StatusOK, gin.H{"storageMode": body.Mode})
}

func (s *Server) setRetrievalMode(c *gin.Context) {
	var body struct{ Mode string `json:"mode"` }
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s.storage.SetRetrievalMode(body.Mode)
	c.JSON(http.StatusOK, gin.H{"retrievalMode": body.Mode})
}

func (s *Server) setMigration(c *gin.Context) {
	var body struct{ Migration bool `json:"migration"` }
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"migration": body.Migration})
}

func (s *Server) setRF(c *gin.Context) {
	var body struct{ ReplicationFactor int `json:"replicationFactor"` }
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s.storage.GroupStorage().UpdateReplicationFactor(body.ReplicationFactor)
	c.JSON(http.StatusOK, gin.H{"replicationFactor": body.ReplicationFactor})
}
