package client

import (
	"strings"
	"sync"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	etcd "github.com/smallnest/libkv-etcdv3-store"
	"github.com/smallnest/rpcx/log"
)

func init() {
	etcd.Register()
}

// EtcdV3Discovery is a etcd service discovery.
// It always returns the registered servers in etcd.
// EtcdV3Discovery 是一个 etcd 发现服务。
// 它总是返回注册到etcd中的registered
type EtcdV3Discovery struct {
	basePath string //   /xes_xueyan_hudong/Classroom
	kv       store.Store
	pairs    []*KVPair
	chans    []chan []*KVPair
	mu       sync.Mutex

	// -1 means it always retry to watch until zookeeper is ok, 0 means no retry.
	RetriesAfterWatchFailed int

	filter ServiceDiscoveryFilter

	stopCh chan struct{}
}

// NewEtcdV3Discovery returns a new EtcdV3Discovery.
// 返回一个新的EtcdV3Discovery
func NewEtcdV3Discovery(basePath string, servicePath string, etcdAddr []string, options *store.Config) ServiceDiscovery {
	kv, err := libkv.NewStore(etcd.ETCDV3, etcdAddr, options) //就是执行 etcdv3.go 的 New 函数
	if err != nil {
		log.Infof("cannot create store: %v", err)
		panic(err)
	}

	return NewEtcdV3DiscoveryStore(basePath+"/"+servicePath, kv)
}

// NewEtcdV3DiscoveryStore return a new EtcdV3Discovery with specified store.
// 返回一个新的有指定的store的EtcdV3Discovery
func NewEtcdV3DiscoveryStore(basePath string, kv store.Store) ServiceDiscovery {
	if len(basePath) > 1 && strings.HasSuffix(basePath, "/") {
		//如果有/后缀，就将/删除？ 比如 /xes_xueyan_hudong/Classroom/ 会改成 /xes_xueyan_hudong/Classroom
		basePath = basePath[:len(basePath)-1]
	}

	d := &EtcdV3Discovery{basePath: basePath, kv: kv}
	d.stopCh = make(chan struct{})

	//这里去找了 以 /xes_xueyan_hudong/Classroom 为前缀的所有的key，然后返回了所有的key value 对
	ps, err := kv.List(basePath) //应该是要去找 /xes_xueyan_hudong/Classroom 下的机器列表
	if err != nil {
		log.Errorf("cannot get services of from registry: %v, err: %v", basePath, err)
		panic(err)
	}
	pairs := make([]*KVPair, 0, len(ps))
	var prefix string
	for _, p := range ps { //遍历key value 对，放在pairs中
		if prefix == "" {
			//这个if else就是获取p.Key的前缀（/xes_xueyan_hudong/Classroom/ 或者 xes_xueyan_hudong/Classroom/）
			if strings.HasPrefix(p.Key, "/") {
				if strings.HasPrefix(d.basePath, "/") {
					prefix = d.basePath + "/"
				} else {
					prefix = "/" + d.basePath + "/"
				}
			} else {
				if strings.HasPrefix(d.basePath, "/") {
					prefix = d.basePath[1:] + "/"
				} else {
					prefix = d.basePath + "/"
				}
			}
		}
		if p.Key == prefix[:len(prefix)-1] {
			continue
		}
		//p.Key过滤掉前缀，原来应该是/xes_xueyan_hudong/Classroom/127.0.0.1:19001 ，去掉前缀后 127.0.0.1:19001
		k := strings.TrimPrefix(p.Key, prefix) //这时候 k 应该是个 ip:port
		pair := &KVPair{Key: k, Value: string(p.Value)}
		if d.filter != nil && !d.filter(pair) {
			continue
		}
		pairs = append(pairs, pair) //填充服务发现的列表
	}
	d.pairs = pairs
	d.RetriesAfterWatchFailed = -1

	go d.watch()
	return d
}

// NewEtcdV3DiscoveryTemplate returns a new EtcdV3Discovery template.
func NewEtcdV3DiscoveryTemplate(basePath string, etcdAddr []string, options *store.Config) ServiceDiscovery {
	if len(basePath) > 1 && strings.HasSuffix(basePath, "/") {
		basePath = basePath[:len(basePath)-1]
	}

	kv, err := libkv.NewStore(etcd.ETCDV3, etcdAddr, options)
	if err != nil {
		log.Infof("cannot create store: %v", err)
		panic(err)
	}

	return &EtcdV3Discovery{basePath: basePath, kv: kv}
}

// Clone clones this ServiceDiscovery with new servicePath.
func (d *EtcdV3Discovery) Clone(servicePath string) ServiceDiscovery {
	return NewEtcdV3DiscoveryStore(d.basePath+"/"+servicePath, d.kv)
}

// SetFilter sets the filer.
func (d *EtcdV3Discovery) SetFilter(filter ServiceDiscoveryFilter) {
	d.filter = filter
}

// GetServices returns the servers
func (d *EtcdV3Discovery) GetServices() []*KVPair {
	return d.pairs
}

// WatchService returns a nil chan.
func (d *EtcdV3Discovery) WatchService() chan []*KVPair {
	d.mu.Lock()
	defer d.mu.Unlock()

	ch := make(chan []*KVPair, 10)
	d.chans = append(d.chans, ch)
	return ch
}

func (d *EtcdV3Discovery) RemoveWatcher(ch chan []*KVPair) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var chans []chan []*KVPair
	for _, c := range d.chans {
		if c == ch {
			continue
		}

		chans = append(chans, c)
	}

	d.chans = chans
}

func (d *EtcdV3Discovery) watch() {
	defer func() {
		d.kv.Close()
	}()

rewatch:
	for {
		var err error
		var c <-chan []*store.KVPair
		var tempDelay time.Duration

		retry := d.RetriesAfterWatchFailed
		for d.RetriesAfterWatchFailed < 0 || retry >= 0 {
			//watchTree 里面启动了个协程，去 watch 以 /xes_xueyan_hudong/Classroom 为前缀的所有的key。然后将watch的变化放进 c 这个chan中
			c, err = d.kv.WatchTree(d.basePath, nil)
			if err != nil {
				if d.RetriesAfterWatchFailed > 0 {
					retry--
				}
				if tempDelay == 0 {
					tempDelay = 1 * time.Second
				} else {
					tempDelay *= 2
				}
				if max := 30 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Warnf("can not watchtree (with retry %d, sleep %v): %s: %v", retry, tempDelay, d.basePath, err)
				time.Sleep(tempDelay)
				continue
			}
			break
		}

		if err != nil {
			log.Errorf("can't watch %s: %v", d.basePath, err)
			return
		}

		for {
			select {
			case <-d.stopCh:
				log.Info("discovery has been closed")
				return
			case ps := <-c: //这里阻塞，循环取c，有变化就更新全部列表。
				if ps == nil {
					log.Warnf("rewatch %s", d.basePath)
					goto rewatch
				}
				var pairs []*KVPair // latest servers
				var prefix string
				for _, p := range ps {
					if prefix == "" {
						if strings.HasPrefix(p.Key, "/") {
							if strings.HasPrefix(d.basePath, "/") {
								prefix = d.basePath + "/"
							} else {
								prefix = "/" + d.basePath + "/"
							}
						} else {
							if strings.HasPrefix(d.basePath, "/") {
								prefix = d.basePath[1:] + "/"
							} else {
								prefix = d.basePath + "/"
							}
						}
					}
					if p.Key == prefix[:len(prefix)-1] || !strings.HasPrefix(p.Key, prefix) {
						continue
					}

					k := strings.TrimPrefix(p.Key, prefix)
					pair := &KVPair{Key: k, Value: string(p.Value)}
					if d.filter != nil && !d.filter(pair) {
						continue
					}
					pairs = append(pairs, pair)
				}
				d.pairs = pairs //更新了服务发现列表

				d.mu.Lock()
				for _, ch := range d.chans {
					ch := ch
					go func() {
						defer func() {
							recover()
						}()

						select {
						case ch <- pairs: //将发现的列表放进chan中
						case <-time.After(time.Minute):
							log.Warn("chan is full and new change has been dropped")
						}
					}()
				}
				d.mu.Unlock()
			}
		}

		log.Warn("chan is closed and will rewatch")
	}
}

func (d *EtcdV3Discovery) Close() {
	close(d.stopCh)
}
