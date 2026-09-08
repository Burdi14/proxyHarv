package main

import (
	"context"
	"crypto/rand"
	"strings"
	"time"

	"proxyfarm/internal/natsb"
	"proxyfarm/internal/parse"
)

func readRandom(b []byte) (int, error) { return rand.Read(b) }

// planLoop builds the check pyramid jobs (§4) every tick.
func (c *coord) planLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.planEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.planJobs(ctx)
		}
	}
}

func (c *coord) planJobs(ctx context.Context) {
	// L0: due nodes with TCP-family transports (cheap dial first, §4)
	if nodes, err := c.store.NodesDueL0(ctx, c.cfg.batchL0*4); err != nil {
		log.Warn("plan L0", "err", err.Error())
	} else if len(nodes) > 0 {
		jobNodes := make([]natsb.JobNode, 0, len(nodes))
		for i := range nodes {
			jn := natsb.JobNode{
				NodeID: nodes[i].ID, Host: nodes[i].Host, Transport: nodes[i].Transport,
			}
			// port is not a column; re-derive from the stored uri
			if p, err := parse.ParseLine(nodes[i].URI); err == nil && p != nil {
				jn.Port = p.Port
			}
			jobNodes = append(jobNodes, jn)
		}
		c.publishBatched(ctx, "L0", "cloud-eu-1", jobNodes, c.cfg.batchL0)
	}

	// L1: due nodes whose transport skips L0 (quic/udp, §4)
	if nodes, err := c.store.NodesDueL1(ctx, c.cfg.batchL1*4); err != nil {
		log.Warn("plan L1", "err", err.Error())
	} else if len(nodes) > 0 {
		jobNodes := make([]natsb.JobNode, 0, len(nodes))
		for i := range nodes {
			if p, err := parse.ParseLine(nodes[i].URI); err == nil && p != nil {
				jobNodes = append(jobNodes, natsb.JobNode{NodeID: nodes[i].ID, Outbound: p.Outbound})
			}
		}
		c.publishBatched(ctx, "L1", "cloud-eu-1", jobNodes, c.cfg.batchL1)
	}

	// L2: top-K alive by score without a recent ok L2 (§4)
	if nodes, err := c.store.NodesForL2(ctx, c.cfg.topL2, 24*time.Hour, c.cfg.batchL2*10); err != nil {
		log.Warn("plan L2", "err", err.Error())
	} else if len(nodes) > 0 {
		jobNodes := make([]natsb.JobNode, 0, len(nodes))
		for i := range nodes {
			if p, err := parse.ParseLine(nodes[i].URI); err == nil && p != nil {
				jobNodes = append(jobNodes, natsb.JobNode{NodeID: nodes[i].ID, Outbound: p.Outbound})
			}
		}
		c.publishBatched(ctx, "L2", "cloud-eu-1", jobNodes, c.cfg.batchL2)
	}

	// validator jobs: top-N cloud-alive per enabled edge vantage (§4)
	vantages, err := c.store.ListVantages(ctx, true)
	if err != nil {
		log.Warn("list vantages", "err", err.Error())
		return
	}
	for _, v := range vantages {
		if v.Kind != "edge" {
			continue
		}
		nodes, err := c.store.NodesForValidation(ctx, v.ID, c.cfg.topVal)
		if err != nil {
			log.Warn("plan VAL", "vantage", v.ID, "err", err.Error())
			continue
		}
		if len(nodes) == 0 {
			continue
		}
		jobNodes := make([]natsb.JobNode, 0, len(nodes))
		for i := range nodes {
			if p, err := parse.ParseLine(nodes[i].URI); err == nil && p != nil {
				jobNodes = append(jobNodes, natsb.JobNode{NodeID: nodes[i].ID, Outbound: p.Outbound})
			}
		}
		// L1 validation for the top-N
		c.publishBatched(ctx, "VAL", v.ID, jobNodes, c.cfg.batchL1)
		// L2 speed for the very best few
		top := c.cfg.valL2Top
		if top > len(jobNodes) {
			top = len(jobNodes)
		}
		if top > 0 {
			c.publishBatched(ctx, "L2", v.ID, jobNodes[:top], c.cfg.batchL2)
		}
	}
}

func (c *coord) publishBatched(ctx context.Context, stage, vantage string, nodes []natsb.JobNode, batch int) {
	published := 0
	for start := 0; start < len(nodes); start += batch {
		end := start + batch
		if end > len(nodes) {
			end = len(nodes)
		}
		job := natsb.JobBatch{
			JobID: newUUID(), Stage: stage, CreatedAt: nowRFC3339(),
			Vantage: vantage, Nodes: nodes[start:end],
		}
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := c.bus.PublishJobs(pctx, stage, vantage, job)
		cancel()
		if err != nil {
			log.Warn("publish jobs", "stage", stage, "err", err.Error())
			return
		}
		published += end - start
		c.cfg.reg.JobBatch("jobs."+strings.ToLower(stage), "published")
	}
	if published > 0 {
		log.Info("planned", "stage", stage, "vantage", vantage, "nodes", published)
	}
}
