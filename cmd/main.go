package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/iggydv12/gomad/internal/config"
	"github.com/iggydv12/gomad/internal/node"
)

var (
	cfgFile  string
	nodeType string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "nomad",
		Short: "Nomad — geo-spatial P2P distributed object storage",
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start a Nomad node",
		RunE:  runStart,
	}

	startCmd.Flags().StringVarP(&nodeType, "type", "t", "peer", "Node type: 'peer' (auto-elects if network is empty) | 'super-peer' (permanent leader, skips discovery)")
	startCmd.Flags().StringVarP(&cfgFile, "config", "c", "", "Path to config file (default: configs/config.yaml)")
	rootCmd.AddCommand(startCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runStart(cmd *cobra.Command, args []string) error {
	// Set up logger
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("logger init: %w", err)
	}
	defer logger.Sync()

	// Load config
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("config load: %w", err)
	}

	// Parse node type
	var role node.ComponentType
	switch nodeType {
	case "super-peer":
		role = node.RoleSuperPeer
	case "peer":
		role = node.RolePeer
	default:
		return fmt.Errorf("unknown node type: %s (use 'peer' or 'super-peer')", nodeType)
	}

	logger.Info("Starting Nomad node", zap.String("type", nodeType))

	ctrl := node.NewController(cfg, role, logger)
	return ctrl.Run(context.Background())
}
