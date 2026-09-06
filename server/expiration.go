package server

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/goccy/bigquery-emulator/internal/contentdata"
	"github.com/goccy/bigquery-emulator/internal/logger"
	"github.com/goccy/bigquery-emulator/internal/metadata"
)

// defaultExpirationSweepInterval is how often the background reaper checks for
// tables whose expirationTime has elapsed.
const defaultExpirationSweepInterval = time.Second

// sweepExpiredTables reclaims every table or view whose expirationTime is at
// or before now. It serializes against REST traffic through the server's
// request lock so a reclaim can never interleave with an in-flight handler
// that cached the table's metadata.
func (s *Server) sweepExpiredTables(ctx context.Context, now time.Time) (int, error) {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	// Content/data operations expect a logger in the context; callers (the
	// background loop, the startup pass and tests) may not provide one.
	ctx = logger.WithLogger(ctx, s.logger)
	return s.reclaimExpiredTables(ctx, now)
}

// reclaimExpiredTables scans all projects and atomically removes every table
// (or view) whose deadline has passed.
//
// Each table is reclaimed in a single transaction that deletes its metadata
// row and drops its backing content table or view together: on commit the
// table disappears from SQL, the Storage Read/Write API, tables.get/list and
// parent listings atomically, and no reader can observe a half-reclaimed
// object. If either step fails the transaction is rolled back, leaving the
// table fully intact so the next sweep (or the startup compensation pass
// after a restart) retries it. Tables without an expirationTime and tables
// whose deadline is still in the future are never touched.
func (s *Server) reclaimExpiredTables(ctx context.Context, now time.Time) (int, error) {
	projects, err := s.metaRepo.FindAllProjects(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to load projects for expiration sweep: %w", err)
	}
	nowMs := now.UnixMilli()
	var reclaimed int
	for _, project := range projects {
		for _, dataset := range project.Datasets() {
			for _, table := range dataset.Tables() {
				if !table.IsExpired(nowMs) {
					continue
				}
				if err := s.reclaimExpiredTable(ctx, project.ID, dataset.ID, table); err != nil {
					// Leave the table untouched and retry it on a later sweep;
					// one bad table must not block reclaiming the others.
					s.logger.Error("failed to reclaim expired table",
						zap.String("project", project.ID),
						zap.String("dataset", dataset.ID),
						zap.String("table", table.ID),
						zap.Error(err),
					)
					continue
				}
				s.logger.Info("reclaimed expired table",
					zap.String("project", project.ID),
					zap.String("dataset", dataset.ID),
					zap.String("table", table.ID),
				)
				reclaimed++
			}
		}
	}
	return reclaimed, nil
}

// reclaimExpiredTable removes one expired table or view. The metadata row and
// the content object (a view for VIEW/MATERIALIZED_VIEW, a table otherwise)
// are dropped in one transaction. The content drop uses IF EXISTS so a
// partially completed reclaim (content already gone, metadata row left
// behind) is still finishable instead of failing forever.
func (s *Server) reclaimExpiredTable(ctx context.Context, projectID, datasetID string, table *metadata.Table) error {
	conn, err := s.connMgr.Connection(ctx, projectID, datasetID)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.RollbackIfNotCommitted()
	// Delete the metadata row first (running at the metadata name path),
	// then drop the content object. Both statements share one transaction
	// and commit atomically.
	if err := table.Delete(ctx, tx.Tx()); err != nil {
		return fmt.Errorf("failed to delete table metadata: %w", err)
	}
	if err := s.contentRepo.DeleteTablesIfExists(
		ctx,
		tx,
		projectID,
		datasetID,
		[]contentdata.TableDeletion{{ID: table.ID, IsView: table.IsView()}},
	); err != nil {
		return fmt.Errorf("failed to drop expired table content: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit table reclaim: %w", err)
	}
	return nil
}

// startExpirationSweeper runs a startup compensation pass for tables that
// already expired (including tables whose deadline elapsed while the process
// was down) and then starts a stoppable background loop that reclaims tables
// as their expirationTime arrives. It is safe to call multiple times; only the
// first call takes effect.
func (s *Server) startExpirationSweeper() {
	s.sweeperOnce.Do(func() {
		interval := s.sweeperInterval
		if interval <= 0 {
			interval = defaultExpirationSweepInterval
		}
		ctx := logger.WithLogger(context.Background(), s.logger)
		// Startup compensation: reclaim everything that is already due
		// before traffic is served, so no expired object survives a restart.
		if n, err := s.sweepExpiredTables(ctx, time.Now()); err != nil {
			s.logger.Error("startup expiration sweep failed", zap.Error(err))
		} else if n > 0 {
			s.logger.Info("startup expiration sweep reclaimed expired tables", zap.Int("count", n))
		}
		stop := make(chan struct{})
		done := make(chan struct{})
		s.sweeperMu.Lock()
		s.sweeperStop = stop
		s.sweeperDone = done
		s.sweeperMu.Unlock()
		go func() {
			defer close(done)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					if _, err := s.sweepExpiredTables(ctx, time.Now()); err != nil {
						s.logger.Error("expiration sweep failed", zap.Error(err))
					}
				}
			}
		}()
	})
}

// stopExpirationSweeper stops the background reaper and waits for an in-flight
// sweep to finish. It is safe to call when the sweeper was never started.
func (s *Server) stopExpirationSweeper() {
	s.sweeperMu.Lock()
	stop := s.sweeperStop
	done := s.sweeperDone
	s.sweeperStop = nil
	s.sweeperDone = nil
	s.sweeperMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	<-done
}
