package rp

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bogdanovich/dns_resolver"
)

// proxyConnection представляет обёртку над httputil.ReverseProxy
// с поддержкой взвешенной балансировки и кастомного транспорта.
type proxyConnection struct {
	reverseProxy *httputil.ReverseProxy
	target       *url.URL
	weight       int
	transport    http.RoundTripper
}

func (p *proxyConnection) String() string {
	return p.target.String()
}

func (p *proxyConnection) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Корректная форвардировка заголовков
	r.URL.Host = p.target.Host
	r.URL.Scheme = p.target.Scheme
	r.Host = p.target.Host
	if r.Header.Get("X-Forwarded-Host") == "" {
		r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	}
	// Транспорт уже инициализирован, повторное присваивание не требуется
	p.reverseProxy.ServeHTTP(w, r)
}

// errorHandler реализует кастомную обработку ошибок проксирования
func (p *proxyConnection) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("proxy error: target=%s, client=%s, error=%v",
		p.target.String(), clientIP(r), err)

	// Классификация ошибок для потенциальной реализации retry-логики
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			http.Error(w, "Upstream timeout", http.StatusGatewayTimeout)
			return
		}
	}

	http.Error(w, "Bad gateway", http.StatusBadGateway)
}

func newProxyConnection(target *url.URL, weight int, transport http.RoundTripper) *proxyConnection {
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.Transport = transport
	rp.ErrorHandler = (&proxyConnection{target: target}).errorHandler

	return &proxyConnection{
		reverseProxy: rp,
		target:       target,
		weight:       weight,
		transport:    transport,
	}
}

// ReverseProxy представляет основной балансировщик с поддержкой round-robin
type ReverseProxy struct {
	mu        sync.RWMutex
	rr        *roundRobin
	log       bool
	transport *http.Transport
}

// New создаёт новый экземпляр прокси с оптимизированными настройками транспорта
func New() *ReverseProxy {
	return &ReverseProxy{
		rr:        newRoundRobin(),
		transport: createOptimizedTransport(nil),
	}
}

// createOptimizedTransport инициализирует http.Transport с параметрами
// для работы в условиях высокой конкурентной нагрузки.
func createOptimizedTransport(dnsServers []string) *http.Transport {
	baseDialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}

	dialContext := baseDialer.DialContext
	if len(dnsServers) > 0 {
		dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			// Кастомный резолвинг только для доменных имён
			if net.ParseIP(host) == nil {
				resolver := dns_resolver.New(dnsServers)
				resolver.RetryTimes = 3
				ips, err := resolver.LookupHost(host)
				if err != nil {
					return nil, fmt.Errorf("dns lookup failed: %w", err)
				}
				if len(ips) > 0 {
					addr = net.JoinHostPort(ips[0].String(), port)
				}
			}
			return baseDialer.DialContext(ctx, network, addr)
		}
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0, // 0 = без ограничений
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableCompression:    false,
	}
}

// ChangeDNS обновляет конфигурацию DNS-резолвера для новых соединений.
// Метод потокобезопасен и не модифицирует глобальное состояние.
func (rp *ReverseProxy) ChangeDNS(domainServers ...string) {
	if len(domainServers) == 0 {
		return
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	if rp.log {
		log.Println("updating DNS configuration:", domainServers)
	}
	rp.transport = createOptimizedTransport(domainServers)
}

// Log включает или отключает режим детального логирования запросов.
func (rp *ReverseProxy) Log(mode bool) {
	rp.log = mode
}

// Add регистрирует новый целевой сервер с указанным весом для балансировки.
func (rp *ReverseProxy) Add(target *url.URL, weight int) {
	rp.mu.RLock()
	transport := rp.transport
	rp.mu.RUnlock()

	rp.rr.Add(newProxyConnection(target, weight, transport))
}

// statusWriter — обёртка над http.ResponseWriter для перехвата статуса ответа.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// ServeHTTP реализует интерфейс http.Handler для обработки входящих запросов.
func (rp *ReverseProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	proxy := rp.rr.Get()
	if proxy == nil {
		http.Error(w, "no upstream available", http.StatusServiceUnavailable)
		return
	}

	if !rp.log {
		proxy.ServeHTTP(w, r)
		return
	}

	// Логирование с замером времени выполнения
	start := time.Now()
	path := r.URL.Path
	if raw := r.URL.RawQuery; raw != "" {
		path += "?" + raw
	}

	sw := &statusWriter{ResponseWriter: w}
	proxy.ServeHTTP(sw, r)

	latency := time.Since(start)
	log.Printf("| %3d | %13v | %15s | %-7s %s -> %s",
		sw.status,
		latency,
		clientIP(r),
		r.Method,
		path,
		proxy.String(),
	)
}

// ListenAndServe запускает HTTP-сервер на указанном адресе.
func (rp *ReverseProxy) ListenAndServe(addr string) error {
	rp.mu.RLock()
	hasTargets := len(rp.rr.conns) > 0
	rp.mu.RUnlock()

	if !hasTargets {
		return fmt.Errorf("not enough remote addresses configured")
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      rp,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	return server.ListenAndServe()
}

// clientIP извлекает реальный IP-адрес клиента из заголовков запроса.
func clientIP(r *http.Request) string {
	// Проверка заголовка X-Forwarded-For
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip := strings.TrimSpace(strings.Split(xff, ",")[0]); ip != "" {
			return ip
		}
	}
	// Проверка заголовка X-Real-IP
	if xri := r.Header.Get("X-Real-Ip"); xri != "" {
		return strings.TrimSpace(xri)
	}
	// Fallback на RemoteAddr
	if ip, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)); err == nil {
		return ip
	}
	return ""
}
