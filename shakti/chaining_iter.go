package shakti

type ChainingIterator struct {
	iters []Iterator
	pos int
}

func (c *ChainingIterator) Current() KV {
	return c.iters[c.pos].Current()
}

func (c *ChainingIterator) Next() error {
	if err := c.iters[c.pos].Next(); err != nil {
		return err
	}
	valid := c.iters[c.pos].IsValid()
	if valid {
		return nil
	}
	c.pos++
	return nil
}

func (c *ChainingIterator) IsValid() bool {
	if c.pos >= len(c.iters) {
		return false
	}
	return c.iters[c.pos].IsValid()
}
