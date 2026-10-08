package clock

import "sync"

// Clock is a Lamport clock: a counter used as the version of each write.
type Clock struct {
	mu   sync.Mutex // one goroutine changes time at a time
	time uint64
}

// Tick: new write, add 1, return it as the version.
func (c *Clock) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.time++
	return c.time
}

// Observe: saw a bigger version from another node, jump up to it.
func (c *Clock) Observe(t uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if t > c.time {
		c.time = t
	}
}
