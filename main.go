package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v5"
)

type Pool struct {
	connString string
	maxConns   int
	mu         sync.Mutex
	conns      []*pgx.Conn
	active     int
}

func NewPool(connString string, maxConns int) *Pool {
	return &Pool{
		connString: connString,
		maxConns:   maxConns,
	}
}

func (p *Pool) Acquire(ctx context.Context) (*pgx.Conn, error) {
	p.mu.Lock()
	for {
		if len(p.conns) > 0 {
			conn := p.conns[len(p.conns)-1]
			p.conns = p.conns[:len(p.conns)-1]
			p.active++
			p.mu.Unlock()
			return conn, nil
		}

		if p.active < p.maxConns {
			p.active++
			p.mu.Unlock()
			conn, err := pgx.Connect(ctx, p.connString)
			if err != nil {
				p.mu.Lock()
				p.active--
				p.mu.Unlock()
				return nil, err
			}
			return conn, nil
		}

		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
			p.mu.Lock()
		}
	}
}

func (p *Pool) Release(conn *pgx.Conn) {
	if conn.IsClosed() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		return
	}

	isBusy := conn.PgConn().IsBusy()
	cleaned := true

	if isBusy {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		cleaned = false
		for {
			msg, err := conn.PgConn().ReceiveMessage(ctx)
			if err != nil {
				conn.Close(context.Background())
				break
			}
			if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
				cleaned = true
				break
			}
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.active--

	if cleaned && !conn.IsClosed() {
		p.conns = append(p.conns, conn)
	}
}

func main() {
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		connString = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	fmt.Println("Connecting to database...")
	pool := NewPool(connString, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		fmt.Printf("Failed to acquire connection: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Executing long-running query...")
	_, err = conn.Exec(ctx, "SELECT pg_sleep(5)")
	if err == nil {
		fmt.Println("Expected query to be cancelled, but it succeeded")
		os.Exit(1)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		fmt.Printf("Expected context cancellation error, got: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Query cancelled successfully as expected")

	fmt.Println("Releasing connection...")
	pool.Release(conn)

	fmt.Println("Acquiring connection again...")
	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		fmt.Printf("Failed to acquire connection second time: %v\n", err)
		os.Exit(1)
	}
	defer pool.Release(conn2)

	fmt.Println("Executing simple query...")
	var val int
	err = conn2.QueryRow(context.Background(), "SELECT 1").Scan(&val)
	if err != nil {
		fmt.Printf("Failed to execute simple query: %v\n", err)
		os.Exit(1)
	}

	if val != 1 {
		fmt.Printf("Expected 1, got %d\n", val)
		os.Exit(1)
	}

	fmt.Println("Success! The second query succeeded without any protocol errors.")
}
