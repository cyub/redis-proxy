package proxy

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	ClusterAddrs  []string
	Addr          string
	DialerTimeout time.Duration
}

func NewConfig() *Config {
	return &Config{
		ReadTimeout:   3 * time.Minute,
		WriteTimeout:  3 * time.Minute,
		ClusterAddrs:  []string{"localhost:6379"},
		Addr:          ":6380",
		DialerTimeout: 3 * time.Second,
	}
}

type Proxy struct {
	config           Config
	conn             *Conn
	clusters         map[string]*Conn
	clusterNames     []string
	clientIDSequence int64
	clients          map[int64]*Client
	transactionChan  chan *transaction
	clusterLock      sync.RWMutex
	clientLock       sync.RWMutex
	wg               *waitGroup
	ht               HashTag
}

func New(config *Config) *Proxy {
	return &Proxy{
		config:          *config,
		transactionChan: make(chan *transaction),
		clients:         make(map[int64]*Client),
		clusters:        make(map[string]*Conn),
		wg:              new(waitGroup),
		ht:              NewHashTag('{', '}'),
	}
}

func (p *Proxy) Run() error {
	if len(p.config.ClusterAddrs) == 0 {
		return errors.New("invalid cluster addrs")
	}

	nl, err := net.Listen("tcp", p.config.Addr)
	if err != nil {
		return err
	}

	errChan := make(chan error)
	for _, clusterAddr := range p.config.ClusterAddrs {
		conf := p.config
		conf.ClusterAddrs = []string{clusterAddr}
		p.wg.Wrap(func() {
			if err := p.ConnectRedis(conf); err != nil {
				errChan <- err
			}
		})
	}
	p.wg.Wait()

	select {
	case err := <-errChan:
		return err
	default:
	}
	go p.Serve()

	for {
		conn, err := nl.Accept()
		if err != nil {
			return err
		}

		go p.HandleServe(conn)
	}
}

func (p *Proxy) ConnectRedis(conf Config) error {
	conn := NewConn(conf)
	if err := conn.Connect(); err != nil {
		return fmt.Errorf("cluster node[%s] error: %s", conf.ClusterAddrs[0], err.Error())
	}
	fmt.Printf("cluster node[%s] connected\n", conf.ClusterAddrs[0])
	p.clusterLock.Lock()
	defer p.clusterLock.Unlock()
	p.clusters[conf.ClusterAddrs[0]] = conn
	p.clusterNames = append(p.clusterNames, conf.ClusterAddrs[0])
	return nil
}

func (p *Proxy) AddClient(id int64, client *Client) {
	p.clientLock.RLock()
	if _, ok := p.clients[id]; ok {
		p.clientLock.RUnlock()
		return
	}

	p.clientLock.RUnlock()
	p.clientLock.Lock()
	defer p.clientLock.Unlock()
	p.clients[id] = client
}

func (p *Proxy) Close() error {
	return nil
}

func (p *Proxy) CloseClient(id int64) {
	p.clientLock.Lock()
	defer p.clientLock.Unlock()
	if client, ok := p.clients[id]; ok {
		client.Close()
		delete(p.clients, id)
	}
}

func (p *Proxy) Serve() {
	for {
		select {
		case trans := <-p.transactionChan:
			if trans.Error != nil {
				continue
			}
			doneChan := trans.doneChan
			resp, err := p.WriteCommand(trans.cmd)
			if err != nil {
				doneChan <- &transaction{Error: err}
			} else {
				doneChan <- &transaction{resp: resp}
			}
		}
	}
}

func (p *Proxy) WriteCommand(cmd *Command) (*Resp, error) {
	conn, err := p.acquireSvcConn(cmd)
	if err != nil {
		return nil, err
	}
	err = conn.WriteCommand(cmd)
	if err != nil {
		return nil, err
	}
	return conn.ReadResponse()
}

func (p *Proxy) acquireSvcConn(cmd *Command) (*Conn, error) {
	var (
		total int
		index int
	)

	if len(cmd.Params) > 0 {
		shardKey := cmd.Params[0]
		leftPos := bytes.IndexByte(shardKey, p.ht.left)
		rightPos := bytes.IndexByte(shardKey, p.ht.right)
		if leftPos >= 0 && rightPos > leftPos+1 {
			shardKey = shardKey[leftPos+1 : rightPos]
		}
		data := md5.Sum(shardKey)
		for _, b := range data {
			total = total + int(b)
		}
		index = total % len(p.clusterNames)
	}

	conn, ok := p.clusters[p.clusterNames[index]]
	if !ok {
		return nil, errors.New("acquire redis conn failure")
	}
	return conn, nil
}

type context struct {
	proxy *Proxy
}

func (p *Proxy) HandleServe(conn net.Conn) {
	fmt.Printf("client: %s connected\n", conn.RemoteAddr())
	clientID := atomic.AddInt64(&p.clientIDSequence, 1)
	client := NewClient(clientID, context{proxy: p}, conn)
	p.AddClient(clientID, client)
	client.IOLoop()
}

type transaction struct {
	cmd      *Command
	resp     *Resp
	doneChan chan *transaction
	Error    error
}

// HashTag struct
type HashTag struct {
	left  byte
	right byte
}

// NewHashTag return hashtag
func NewHashTag(left, right byte) HashTag {
	return HashTag{left, right}
}

type waitGroup struct {
	sync.WaitGroup
}

func (w *waitGroup) Wrap(fn func()) {
	w.WaitGroup.Add(1)
	go func() {
		defer w.WaitGroup.Done()
		fn()
	}()
}
