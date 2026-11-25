package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/intervalpli"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

var defaultStunServers = []string{
	"stun:stun.l.google.com:19302",
}

func getStunServers() []string {
	if env := os.Getenv("STUN_SERVERS"); env != "" {
		return strings.Split(env, ",")
	}
	return defaultStunServers
}

func parseCSVEnv(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func getIceServers() []webrtc.ICEServer {
	iceServers := []webrtc.ICEServer{}
	for _, stun := range getStunServers() {
		if trimmed := strings.TrimSpace(stun); trimmed != "" {
			iceServers = append(iceServers, webrtc.ICEServer{URLs: []string{trimmed}})
		}
	}

	turnURLs := parseCSVEnv("TURN_URLS")
	if len(turnURLs) == 0 {
		turnURLs = parseCSVEnv("TURN_URL")
	}
	if len(turnURLs) > 0 {
		user := envOrDefault("TURN_USER", os.Getenv("TURN_USERNAME"))
		pass := envOrDefault("TURN_PASS", os.Getenv("TURN_CREDENTIAL"))
		iceServers = append(iceServers, webrtc.ICEServer{
			URLs:           turnURLs,
			Username:       user,
			Credential:     pass,
			CredentialType: webrtc.ICECredentialTypePassword,
		})
	}

	return iceServers
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getFfmpegPath() string {
	return envOrDefault("FFMPEG_PATH", "ffmpeg")
}

func debugLogsEnabled() bool {
	return os.Getenv("SIDECAR_DEBUG_LOGS") == "1"
}

func debugf(format string, args ...any) {
	if debugLogsEnabled() {
		log.Printf(format, args...)
	}
}

// NTP epoch offset: seconds between 1900-01-01 and 1970-01-01
const ntpEpochOffset = 2208988800

func toNTPTime(t time.Time) uint64 {
	secs := uint64(t.Unix()) + ntpEpochOffset
	frac := uint64(t.Nanosecond()) * (1 << 32) / 1e9
	return secs<<32 | frac
}

func isVP8KeyframeStart(payload []byte) bool {
	if len(payload) < 2 {
		return false
	}
	i := 0

	// VP8 payload descriptor
	b0 := payload[i]
	x := (b0 & 0x80) != 0
	s := (b0 & 0x10) != 0
	pid := b0 & 0x0F
	i++

	if !s || pid != 0 {
		return false
	}

	if x {
		if len(payload) <= i {
			return false
		}
		ext := payload[i]
		i++

		if (ext & 0x80) != 0 {
			if len(payload) <= i {
				return false
			}

			// M bit => 16-bit PictureID, else 8-bit
			if (payload[i] & 0x80) != 0 {
				i += 2
			} else {
				i += 1
			}
		}

		// L: TL0PICIDX present
		if (ext & 0x40) != 0 {
			i += 1
		}

		// T or K => one extra octet
		if (ext&0x20) != 0 || (ext&0x10) != 0 {
			i += 1
		}
	}
	if len(payload) <= i {
		return false
	}
	// VP8 frame tag: bit 0 == frame type
	// 0 = keyframe, 1 = interframe
	return (payload[i] & 0x01) == 0
}

func rtpElapsed(ts, base, clockRate uint32) time.Duration {
	return time.Duration((uint64(ts-base) * uint64(time.Second)) / uint64(clockRate))
}

func smoothDuration(prev, sample time.Duration) time.Duration {
	if prev <= 0 {
		return sample
	}
	return (prev*9 + sample) / 10
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (s *Sidecar) resetSyncTiming() {
	s.timingMu.Lock()
	defer s.timingMu.Unlock()

	s.streamBaseWall = time.Time{}
	s.streamBaseSet = false
	s.videoTiming = TrackTiming{}
	s.audioTiming = TrackTiming{}
}

func (s *Sidecar) drainRTPQueues() {
	for {
		select {
		case <-s.videoQueue:
		default:
			goto drainAudio
		}
	}

drainAudio:
	for {
		select {
		case <-s.audioQueue:
		default:
			return
		}
	}
}

func (s *Sidecar) resetPeerStreamState() {
	s.peersLock.RLock()
	defer s.peersLock.RUnlock()

	for _, peer := range s.peers {
		peer.mu.Lock()
		peer.Started = false
		peer.mu.Unlock()
		peer.drainQueues()
	}
}

type queuedRTP struct {
	pkt    *rtp.Packet
	rxTime time.Time
}

func (s *Sidecar) computeTrackDelay(kind string, ts uint32, rxTime time.Time, now time.Time) time.Duration {
	s.timingMu.Lock()
	defer s.timingMu.Unlock()

	if !s.streamBaseSet {
		s.streamBaseSet = true
		s.streamBaseWall = rxTime
	}

	var current *TrackTiming
	var other *TrackTiming
	var clockRate uint32

	switch kind {
	case "video":
		current = &s.videoTiming
		other = &s.audioTiming
		clockRate = 90000
	case "audio":
		current = &s.audioTiming
		other = &s.videoTiming
		clockRate = 48000
	default:
		return 0
	}

	if !current.initialized {
		current.initialized = true
		current.baseRTP = ts
	}

	mediaElapsed := rtpElapsed(ts, current.baseRTP, clockRate)
	expectedWall := s.streamBaseWall.Add(mediaElapsed)

	observedLatency := rxTime.Sub(expectedWall)
	if observedLatency < 0 {
		observedLatency = 0
	} else if observedLatency > 300*time.Millisecond {
		observedLatency = 300 * time.Millisecond
	}

	current.latency = smoothDuration(current.latency, observedLatency)

	targetLatency := current.latency
	if other.initialized {
		targetLatency = maxDuration(targetLatency, other.latency)
	}

	targetWall := expectedWall.Add(targetLatency).Add(s.syncBuffer)
	if kind == "video" {
		targetWall = targetWall.Add(s.videoBias)
	}

	delay := targetWall.Sub(now)
	if delay < 0 {
		return 0
	}
	if delay > 100*time.Millisecond {
		return 100 * time.Millisecond
	}

	return delay
}

func cloneRTPPacket(src *rtp.Packet) *rtp.Packet {
	raw, err := src.Marshal()
	if err != nil {
		return nil
	}

	dst := &rtp.Packet{}
	if err := dst.Unmarshal(raw); err != nil {
		return nil
	}

	return dst
}

type TrackTiming struct {
	initialized bool
	baseRTP     uint32
	latency     time.Duration
}

type createInFlight struct {
	done chan struct{}
	sdp  string
	err  error
}

type Peer struct {
	ID         string
	PC         *webrtc.PeerConnection
	VideoTrack *webrtc.TrackLocalStaticRTP
	AudioTrack *webrtc.TrackLocalStaticRTP
	VideoSSRC  uint32
	AudioSSRC  uint32
	Active     bool
	Started    bool
	mu         sync.Mutex
	stopSR     chan struct{}
	closeOnce  sync.Once
	videoSend  chan *rtp.Packet
	audioSend  chan *rtp.Packet
	pendingICE []webrtc.ICECandidateInit
	droppedVid uint64
	droppedAud uint64
}

func (p *Peer) Close() {
	p.mu.Lock()
	p.Active = false
	p.Started = false
	p.mu.Unlock()

	p.closeOnce.Do(func() {
		close(p.stopSR)
		if p.PC != nil {
			_ = p.PC.Close()
		}
	})
}

func (p *Peer) drainQueues() {
	for {
		select {
		case <-p.videoSend:
		default:
			goto drainAudio
		}
	}

drainAudio:
	for {
		select {
		case <-p.audioSend:
		default:
			return
		}
	}
}

func (p *Peer) enqueueVideo(pkt *rtp.Packet) {
	if pkt == nil || p.VideoTrack == nil {
		return
	}
	select {
	case <-p.stopSR:
		return
	default:
	}
	select {
	case p.videoSend <- pkt:
	default:
		n := atomic.AddUint64(&p.droppedVid, 1)
		if n == 1 || n%120 == 0 {
			log.Printf("[Peer %s] Video send queue full (dropped=%d)", p.ID, n)
		}
	}
}

func (p *Peer) enqueueAudio(pkt *rtp.Packet) {
	if pkt == nil || p.AudioTrack == nil {
		return
	}
	select {
	case <-p.stopSR:
		return
	default:
	}
	select {
	case p.audioSend <- pkt:
	default:
		n := atomic.AddUint64(&p.droppedAud, 1)
		if n == 1 || n%200 == 0 {
			log.Printf("[Peer %s] Audio send queue full (dropped=%d)", p.ID, n)
		}
	}
}

func (p *Peer) runVideoSender() {
	for {
		select {
		case <-p.stopSR:
			return
		case pkt := <-p.videoSend:
			if pkt != nil && p.VideoTrack != nil {
				_ = p.VideoTrack.WriteRTP(pkt)
			}
		}
	}
}

func (p *Peer) runAudioSender() {
	for {
		select {
		case <-p.stopSR:
			return
		case pkt := <-p.audioSend:
			if pkt != nil && p.AudioTrack != nil {
				_ = p.AudioTrack.WriteRTP(pkt)
			}
		}
	}
}

type Sidecar struct {
	peers     map[string]*Peer
	peersLock sync.RWMutex
	creating  map[string]*createInFlight

	videoPort int
	audioPort int
	videoConn *net.UDPConn
	audioConn *net.UDPConn

	ffmpeg     *exec.Cmd
	ffmpegLock sync.Mutex
	source     string
	running    bool

	// Atomic timestamps for RTCP Sender Report generation
	lastVideoRTPTs uint64 // atomic: latest video RTP timestamp seen
	lastAudioRTPTs uint64 // atomic: latest audio RTP timestamp seen
	videoPktCount  uint64 // atomic
	videOctetCount uint64 // atomic
	audioPktCount  uint64 // atomic
	audioOctetCount uint64 // atomic
	
	videoQueue chan queuedRTP
	audioQueue chan queuedRTP

	// Stream pacing / A/V alignment state
	timingMu       sync.Mutex
	streamBaseWall time.Time
	streamBaseSet  bool
	videoTiming    TrackTiming
	audioTiming    TrackTiming
	syncBuffer     time.Duration
	videoBias      time.Duration
	paceBurst      int
	paceStep       time.Duration
	maxFramePace   time.Duration
}

func NewSidecar() *Sidecar {
	paceBurst := envIntOrDefault("VIDEO_PACE_BURST", 4)
	if paceBurst < 0 {
		paceBurst = 0
	}
	paceStepMs := envIntOrDefault("VIDEO_PACE_STEP_MS", 2)
	if paceStepMs < 0 {
		paceStepMs = 0
	}
	maxPaceMs := envIntOrDefault("VIDEO_MAX_PACE_MS", 12)
	if maxPaceMs < 0 {
		maxPaceMs = 0
	}

	return &Sidecar{
		peers:        make(map[string]*Peer),
		creating:     make(map[string]*createInFlight),
		syncBuffer:   time.Duration(envIntOrDefault("SYNC_PLAYOUT_BUFFER_MS", 50)) * time.Millisecond,
		videoBias:    time.Duration(envIntOrDefault("SYNC_VIDEO_BIAS_MS", 0)) * time.Millisecond,
		paceBurst:    paceBurst,
		paceStep:     time.Duration(paceStepMs) * time.Millisecond,
		maxFramePace: time.Duration(maxPaceMs) * time.Millisecond,
		videoQueue:   make(chan queuedRTP, envIntOrDefault("VIDEO_QUEUE_SIZE", 1024)),
		audioQueue:   make(chan queuedRTP, envIntOrDefault("AUDIO_QUEUE_SIZE", 2048)),
	}
}


func (s *Sidecar) StartRTP() error {
	var err error

	s.videoConn, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		return fmt.Errorf("bind video UDP: %w", err)
	}
	_ = s.videoConn.SetReadBuffer(envIntOrDefault("VIDEO_RTP_READ_BUFFER", 4*1024*1024))
	s.videoPort = s.videoConn.LocalAddr().(*net.UDPAddr).Port

	s.audioConn, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		return fmt.Errorf("bind audio UDP: %w", err)
	}
	_ = s.audioConn.SetReadBuffer(envIntOrDefault("AUDIO_RTP_READ_BUFFER", 1*1024*1024))
	s.audioPort = s.audioConn.LocalAddr().(*net.UDPAddr).Port

	log.Printf("[RTP] Video port: %d, Audio port: %d", s.videoPort, s.audioPort)
	s.running = true

	go s.readVideoRTP()
	go s.readAudioRTP()
	go s.processVideoRTP()
	go s.processAudioRTP()

	return nil
}

func (s *Sidecar) readVideoRTP() {
	buf := make([]byte, 1500)
	pkt := &rtp.Packet{}
	count := 0

	for s.running {
		n, err := s.videoConn.Read(buf)
		if err != nil {
			if s.running {
				log.Printf("[RTP] Video read error: %v", err)
			}
			return
		}

		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		rxTime := time.Now()

		// Track RTP stats used by optional debug / legacy reporting paths
		atomic.StoreUint64(&s.lastVideoRTPTs, uint64(pkt.Timestamp))
		atomic.AddUint64(&s.videoPktCount, 1)
		atomic.AddUint64(&s.videOctetCount, uint64(len(pkt.Payload)))

		count++
		if count <= 3 || count%600 == 0 {
			debugf("[VIDEO] #%d ts=%d (%.3fs) marker=%v", count, pkt.Timestamp, float64(pkt.Timestamp)/90000.0, pkt.Marker)
		}

		cloned := cloneRTPPacket(pkt)
		if cloned == nil {
			continue
		}

		select {
		case s.videoQueue <- queuedRTP{pkt: cloned, rxTime: rxTime}:
		default:
			if count%120 == 0 {
				log.Printf("[VIDEO] queue full, dropping packet ts=%d", cloned.Timestamp)
			}
		}
	}
}

func (s *Sidecar) readAudioRTP() {
	buf := make([]byte, 1500)
	pkt := &rtp.Packet{}
	count := 0

	for s.running {
		n, err := s.audioConn.Read(buf)
		if err != nil {
			if s.running {
				log.Printf("[RTP] Audio read error: %v", err)
			}
			return
		}

		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		rxTime := time.Now()

		// Track latest timestamp for RTCP Sender Reports
		atomic.StoreUint64(&s.lastAudioRTPTs, uint64(pkt.Timestamp))
		atomic.AddUint64(&s.audioPktCount, 1)
		atomic.AddUint64(&s.audioOctetCount, uint64(len(pkt.Payload)))

		count++
		if count <= 3 || count%1000 == 0 {
			debugf("[AUDIO] #%d ts=%d (%.3fs)", count, pkt.Timestamp, float64(pkt.Timestamp)/48000.0)
		}

		cloned := cloneRTPPacket(pkt)
		if cloned == nil {
			continue
		}

		select {
		case s.audioQueue <- queuedRTP{pkt: cloned, rxTime: rxTime}:
		default:
			if count%200 == 0 {
				log.Printf("[AUDIO] queue full, dropping packet ts=%d", cloned.Timestamp)
			}
		}
	}
}

func (s *Sidecar) processVideoRTP() {
	var lastTS uint32
	haveTS := false
	var frameStart time.Time
	burstCount := 0

	for item := range s.videoQueue {
		pkt := item.pkt
		if !haveTS || pkt.Timestamp != lastTS {
			now := time.Now()
			extraDelay := s.computeTrackDelay("video", pkt.Timestamp, item.rxTime, now)
			if extraDelay > 0 {
				time.Sleep(extraDelay)
			}
			lastTS = pkt.Timestamp
			haveTS = true
			frameStart = time.Now()
			burstCount = 0
		} else if burstCount > 0 && s.maxFramePace > 0 && s.paceBurst > 0 && s.paceStep > 0 && burstCount%s.paceBurst == 0 {
			// Spread intra-frame bursts (especially keyframes) slightly without building queue backlog
			if time.Since(frameStart) < s.maxFramePace && len(s.videoQueue) < 64 {
				time.Sleep(s.paceStep)
			}
		}

		s.peersLock.RLock()
		for _, peer := range s.peers {
			peer.mu.Lock()
			active := peer.Active
			started := peer.Started

			if active && !started && isVP8KeyframeStart(pkt.Payload) {
				peer.Started = true
				started = true
				log.Printf("[Peer %s] First VP8 keyframe seen at ts=%d - opening stream gate", peer.ID, pkt.Timestamp)
			}

			peer.mu.Unlock()

			if active && started {
				if peerPkt := cloneRTPPacket(pkt); peerPkt != nil {
					peer.enqueueVideo(peerPkt)
				}
			}
		}
		s.peersLock.RUnlock()
		burstCount++
	}
}

func (s *Sidecar) processAudioRTP() {
	var lastTS uint32
	haveTS := false

	for item := range s.audioQueue {
		pkt := item.pkt
		if !haveTS || pkt.Timestamp != lastTS {
			now := time.Now()
			extraDelay := s.computeTrackDelay("audio", pkt.Timestamp, item.rxTime, now)
			if extraDelay > 0 {
				time.Sleep(extraDelay)
			}
			lastTS = pkt.Timestamp
			haveTS = true
		}

		s.peersLock.RLock()
		for _, peer := range s.peers {
			peer.mu.Lock()
			active := peer.Active
			started := peer.Started
			peer.mu.Unlock()

			if active && started {
				if peerPkt := cloneRTPPacket(pkt); peerPkt != nil {
					peer.enqueueAudio(peerPkt)
				}
			}
		}
		s.peersLock.RUnlock()
	}
}

func normalizeOfferSDP(raw string) string {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	rawLines := strings.Split(raw, "\n")

	var sessionLines []string
	var mediaSections [][]string
	var currentMedia []string
	inMedia := false
	sessionFingerprint := ""

	for _, line := range rawLines {
		line = strings.TrimRight(line, " \t")
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "m=") {
			if inMedia {
				mediaSections = append(mediaSections, currentMedia)
			}
			inMedia = true
			currentMedia = []string{line}
			continue
		}
		if !inMedia {
			if strings.HasPrefix(line, "o=") && strings.HasSuffix(line, " IN IP4 0.0.0.0") {
				line = strings.TrimSuffix(line, " IN IP4 0.0.0.0") + " IN IP4 127.0.0.1"
			} else if line == "a=msid-semantic:WMS*" || line == "a=msid-semantic:WMS" {
				line = "a=msid-semantic: WMS ts6-stream"
			} else if strings.HasPrefix(line, "a=fingerprint:") {
				sessionFingerprint = line
				continue
			}
			sessionLines = append(sessionLines, line)
		} else {
			currentMedia = append(currentMedia, line)
		}
	}
	if inMedia {
		mediaSections = append(mediaSections, currentMedia)
	}

	out := make([]string, 0, len(rawLines)+6)
	out = append(out, sessionLines...)

	for _, sec := range mediaSections {
		hasRTCP := false
		hasIceOptions := false
		hasFingerprint := false
		for _, l := range sec {
			if strings.HasPrefix(l, "a=rtcp:") {
				hasRTCP = true
			} else if strings.HasPrefix(l, "a=ice-options:") {
				hasIceOptions = true
			} else if strings.HasPrefix(l, "a=fingerprint:") {
				hasFingerprint = true
			}
		}

		for _, l := range sec {
			if strings.HasPrefix(l, "a=ssrc:") &&
				(strings.Contains(l, " mslabel:") || strings.Contains(l, " label:")) {
				continue
			}
			if strings.HasPrefix(l, "a=candidate:") {
				fields := strings.Fields(l)
				if len(fields) >= 2 && fields[1] == "2" {
					continue
				}
			}

			out = append(out, l)

			if strings.HasPrefix(l, "c=IN ") && !hasRTCP {
				out = append(out, "a=rtcp:9 IN IP4 0.0.0.0")
				hasRTCP = true
			}
			if strings.HasPrefix(l, "a=ice-pwd:") {
				if !hasIceOptions {
					out = append(out, "a=ice-options:trickle")
					hasIceOptions = true
				}
				if !hasFingerprint && sessionFingerprint != "" {
					out = append(out, sessionFingerprint)
					hasFingerprint = true
				}
			}
		}
	}

	return strings.Join(out, "\r\n") + "\r\n"
}

func (s *Sidecar) CreatePeer(id string) (sdp string, err error) {
	s.peersLock.Lock()

	// If a create for this ID is already in progress, wait for it FIRST.
	if inflight, exists := s.creating[id]; exists {
		s.peersLock.Unlock()
		debugf("[API] Waiting for in-flight peer creation: %s", id)
		<-inflight.done
		return inflight.sdp, inflight.err
	}

	// Reuse existing peer/offer only when no create is currently in flight.
	if existing, exists := s.peers[id]; exists {
		state := existing.PC.ICEConnectionState()
		if state != webrtc.ICEConnectionStateClosed &&
			state != webrtc.ICEConnectionStateFailed &&
			state != webrtc.ICEConnectionStateDisconnected {
			if ld := existing.PC.LocalDescription(); ld != nil {
				s.peersLock.Unlock()
				debugf("[API] Reusing existing peer offer: %s", id)
				return ld.SDP, nil
			}
		}
	}

	inflight := &createInFlight{done: make(chan struct{})}
	s.creating[id] = inflight
	s.peersLock.Unlock()

	log.Printf("[API] Creating NEW peer: %s", id)
	startTime := time.Now()

	defer func() {
		s.peersLock.Lock()
		inflight.sdp = sdp
		inflight.err = err
		delete(s.creating, id)
		close(inflight.done)
		s.peersLock.Unlock()
	}()

	m := &webrtc.MediaEngine{}
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeVP8,
			ClockRate:   90000,
			SDPFmtpLine: "",
		},
		PayloadType: 96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return "", err
	}
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1;stereo=1;sprop-stereo=1",
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return "", err
	}

	i := &interceptor.Registry{}
	intervalPliFactory, err := intervalpli.NewReceiverInterceptor()
	if err != nil {
		return "", err
	}
	i.Add(intervalPliFactory)
	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		return "", err
	}

	se := webrtc.SettingEngine{}
	udpMin := envIntOrDefault("WEBRTC_UDP_MIN", 0)
	udpMax := envIntOrDefault("WEBRTC_UDP_MAX", 0)
	if udpMin > 0 && udpMax >= udpMin && udpMax <= 65535 {
		if err := se.SetEphemeralUDPPortRange(uint16(udpMin), uint16(udpMax)); err != nil {
			log.Printf("[API] Warning: SetEphemeralUDPPortRange(%d, %d) failed: %v", udpMin, udpMax, err)
		}
	}
	if publicIPs := parseCSVEnv("WEBRTC_PUBLIC_IP"); len(publicIPs) > 0 {
		se.SetNAT1To1IPs(publicIPs, webrtc.ICECandidateTypeHost)
	}

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(m),
		webrtc.WithInterceptorRegistry(i),
		webrtc.WithSettingEngine(se),
	)

	cfg := webrtc.Configuration{
		ICEServers:    getIceServers(),
		RTCPMuxPolicy: webrtc.RTCPMuxPolicyRequire,
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("ICE_TRANSPORT_POLICY")), "relay") {
		cfg.ICETransportPolicy = webrtc.ICETransportPolicyRelay
	}

	pc, err := api.NewPeerConnection(cfg)
	if err != nil {
		return "", fmt.Errorf("create PeerConnection: %w", err)
	}

	videoTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		"video", "ts6-stream",
	)
	if err != nil {
		pc.Close()
		return "", err
	}

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "ts6-stream",
	)
	if err != nil {
		pc.Close()
		return "", err
	}

	if _, err = pc.AddTransceiverFromTrack(videoTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		pc.Close()
		return "", err
	}
	if _, err = pc.AddTransceiverFromTrack(audioTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		pc.Close()
		return "", err
	}

	peer := &Peer{
		ID:         id,
		PC:         pc,
		VideoTrack: videoTrack,
		AudioTrack: audioTrack,
		Active:     false,
		stopSR:     make(chan struct{}),
		videoSend:  make(chan *rtp.Packet, envIntOrDefault("PEER_VIDEO_QUEUE_SIZE", 256)),
		audioSend:  make(chan *rtp.Packet, envIntOrDefault("PEER_AUDIO_QUEUE_SIZE", 256)),
	}

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("[Peer %s] ICE: %s", id, state.String())
		switch state {
		case webrtc.ICEConnectionStateConnected:
			peer.mu.Lock()
			peer.Active = true
			peer.Started = false
			peer.mu.Unlock()
			// Resolve SSRCs NOW — they are only valid after negotiation
			for _, sender := range pc.GetSenders() {
				params := sender.GetParameters()
				if len(params.Encodings) > 0 {
					ssrc := uint32(params.Encodings[0].SSRC)
					if sender.Track() == videoTrack {
						peer.VideoSSRC = ssrc
						log.Printf("[Peer %s] Video SSRC resolved: %d", id, ssrc)
					} else if sender.Track() == audioTrack {
						peer.AudioSSRC = ssrc
						log.Printf("[Peer %s] Audio SSRC resolved: %d", id, ssrc)
					}
				}
			}
		case webrtc.ICEConnectionStateDisconnected, webrtc.ICEConnectionStateFailed, webrtc.ICEConnectionStateClosed:
			peer.mu.Lock()
			peer.Active = false
			peer.Started = false
			peer.mu.Unlock()
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return "", fmt.Errorf("create offer: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return "", fmt.Errorf("set local desc: %w", err)
	}

	gatherComplete := webrtc.GatheringCompletePromise(pc)
	gatherTimeout := time.Duration(envIntOrDefault("ICE_GATHER_TIMEOUT_MS", 2500)) * time.Millisecond
	select {
	case <-gatherComplete:
	case <-time.After(gatherTimeout):
		log.Printf("[Peer %s] ICE gathering timed out after %v, proceeding with gathered candidates", id, gatherTimeout)
	}

	ld := pc.LocalDescription()
	if ld == nil {
		_ = pc.Close()
		return "", fmt.Errorf("missing local description after ICE gathering")
	}

	go peer.runVideoSender()
	go peer.runAudioSender()

	s.peersLock.Lock()
	if old, exists := s.peers[id]; exists {
		old.Close()
	}
	s.peers[id] = peer
	s.peersLock.Unlock()

	sdp = normalizeOfferSDP(ld.SDP)
	log.Printf("[Peer %s] Offer ready in %dms (%d bytes)", id, time.Since(startTime).Milliseconds(), len(sdp))
	return sdp, nil
}

// sendSenderReports periodically sends RTCP Sender Reports with synchronized
// NTP timestamps for both audio and video, enabling the browser to correlate
// the two RTP clocks and maintain lip-sync.
func (s *Sidecar) sendSenderReports(peer *Peer) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	cname := "ts6-stream"
	srCount := 0

	for {
		select {
		case <-peer.stopSR:
			return
		case <-ticker.C:
			if !peer.Active {
				continue
			}

			now := time.Now()
			ntpNow := toNTPTime(now)

			videoTs := uint32(atomic.LoadUint64(&s.lastVideoRTPTs))
			audioTs := uint32(atomic.LoadUint64(&s.lastAudioRTPTs))
			vidPkts := uint32(atomic.LoadUint64(&s.videoPktCount))
			vidOctets := uint32(atomic.LoadUint64(&s.videOctetCount))
			audPkts := uint32(atomic.LoadUint64(&s.audioPktCount))
			audOctets := uint32(atomic.LoadUint64(&s.audioOctetCount))

			if videoTs == 0 && audioTs == 0 {
				continue
			}

			srCount++

			// Send video SR + SDES
			if peer.VideoSSRC != 0 {
				err := peer.PC.WriteRTCP([]rtcp.Packet{
					&rtcp.SenderReport{
						SSRC:        peer.VideoSSRC,
						NTPTime:     ntpNow,
						RTPTime:     videoTs,
						PacketCount: vidPkts,
						OctetCount:  vidOctets,
					},
					&rtcp.SourceDescription{
						Chunks: []rtcp.SourceDescriptionChunk{{
							Source: peer.VideoSSRC,
							Items: []rtcp.SourceDescriptionItem{{
								Type: rtcp.SDESCNAME,
								Text: cname,
							}},
						}},
					},
				})
				if srCount <= 5 || srCount%30 == 0 {
					log.Printf("[SR] Peer %s video SR #%d ssrc=%d rtpTs=%d err=%v", peer.ID, srCount, peer.VideoSSRC, videoTs, err)
				}
			} else if srCount <= 5 {
				log.Printf("[SR] Peer %s video SSRC still 0 — skipping SR", peer.ID)
			}

			// Send audio SR + SDES with SAME NTP time and SAME CNAME
			if peer.AudioSSRC != 0 {
				err := peer.PC.WriteRTCP([]rtcp.Packet{
					&rtcp.SenderReport{
						SSRC:        peer.AudioSSRC,
						NTPTime:     ntpNow,
						RTPTime:     audioTs,
						PacketCount: audPkts,
						OctetCount:  audOctets,
					},
					&rtcp.SourceDescription{
						Chunks: []rtcp.SourceDescriptionChunk{{
							Source: peer.AudioSSRC,
							Items: []rtcp.SourceDescriptionItem{{
								Type: rtcp.SDESCNAME,
								Text: cname,
							}},
						}},
					},
				})
				if srCount <= 5 || srCount%30 == 0 {
					log.Printf("[SR] Peer %s audio SR #%d ssrc=%d rtpTs=%d err=%v", peer.ID, srCount, peer.AudioSSRC, audioTs, err)
				}
			} else if srCount <= 5 {
				log.Printf("[SR] Peer %s audio SSRC still 0 — skipping SR", peer.ID)
			}
		}
	}
}

func (s *Sidecar) SetAnswer(id, sdp string) error {
	s.peersLock.RLock()
	peer, exists := s.peers[id]
	s.peersLock.RUnlock()
	if !exists {
		return fmt.Errorf("peer %s not found", id)
	}

	peer.mu.Lock()

	if peer.PC.RemoteDescription() != nil {
		if peer.PC.RemoteDescription().Type == webrtc.SDPTypeAnswer &&
			peer.PC.SignalingState() == webrtc.SignalingStateStable {
			peer.mu.Unlock()
			log.Printf("[API] Ignoring duplicate answer for peer: %s", id)
			return nil
		}
	}

	if peer.PC.SignalingState() != webrtc.SignalingStateHaveLocalOffer {
		state := peer.PC.SignalingState()
		peer.mu.Unlock()
		log.Printf("[API] Ignoring answer in signaling state %s for peer: %s", state, id)
		return nil
	}

	if err := peer.PC.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	}); err != nil {
		peer.mu.Unlock()
		return err
	}

	queued := peer.pendingICE
	peer.pendingICE = nil
	peer.mu.Unlock()

	for _, cand := range queued {
		if err := peer.PC.AddICECandidate(cand); err != nil {
			log.Printf("[API] Flushed ICE error (peer=%s): %v", id, err)
		}
	}

	return nil
}

func (s *Sidecar) AddICECandidate(id string, candidate string, sdpMid string, sdpMLineIndex uint16) error {
	s.peersLock.RLock()
	peer, exists := s.peers[id]
	s.peersLock.RUnlock()
	if !exists {
		return fmt.Errorf("peer %s not found", id)
	}

	init := webrtc.ICECandidateInit{
		Candidate:     candidate,
		SDPMid:        &sdpMid,
		SDPMLineIndex: &sdpMLineIndex,
	}

	peer.mu.Lock()
	if peer.PC.RemoteDescription() == nil {
		peer.pendingICE = append(peer.pendingICE, init)
		peer.mu.Unlock()
		return nil
	}
	peer.mu.Unlock()

	return peer.PC.AddICECandidate(init)
}

func (s *Sidecar) ClosePeer(id string) {
	s.peersLock.Lock()
	if peer, exists := s.peers[id]; exists {
		peer.Close()
		delete(s.peers, id)
	}
	s.peersLock.Unlock()
}

func (s *Sidecar) StartFFmpeg(source string, width int, height int, framerate int, bitrate string) {
	s.ffmpegLock.Lock()
	defer s.ffmpegLock.Unlock()

	s.StopFFmpegLocked()
	s.resetSyncTiming()
	s.drainRTPQueues()
	s.resetPeerStreamState()

	s.source = source

	w := width
	h := height
	fps := framerate

	if w <= 0 {
		w = envIntOrDefault("VIDEO_WIDTH", 1280)
	}

	if h <= 0 {
		h = envIntOrDefault("VIDEO_HEIGHT", 720)
	}

	if fps <= 0 {
		fps = envIntOrDefault("VIDEO_FRAMERATE", 30)
	}

	args := []string{}

	if source != "" {
		if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
			args = append(args, "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5")
		} else {
			args = append(args, "-stream_loop", "-1")
		}

		args = append(args, "-fflags", "+genpts+discardcorrupt", "-re", "-i", source)
	} else {
		args = append(args, "-re", "-f", "lavfi", "-i", fmt.Sprintf("color=c=black:s=%dx%d:r=1", w, h))
	}

	vBitrate := strings.TrimSpace(bitrate)
	if vBitrate == "" {
		vBitrate = envOrDefault("VIDEO_BITRATE", "1500k")
	}
	vBufsize := strings.TrimSpace(os.Getenv("VIDEO_BUFSIZE"))
	if vBufsize == "" {
		vBufsize = vBitrate
	}
	gop := envIntOrDefault("VIDEO_GOP", fps)
	if gop < 1 {
		gop = fps
	}
	gopStr := strconv.Itoa(gop)
	cpuUsed := envOrDefault("VIDEO_CPU_USED", "6")
	audioDelayMs := envIntOrDefault("AUDIO_DELAY_MS", 0)

	if source != "" {
		vf := fmt.Sprintf(
			"fps=%d,scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
			fps, w, h, w, h,
		)
		args = append(args,
			"-map", "0:v:0",
			"-vf", vf,
		)
	}
	args = append(args,
		"-pix_fmt", "yuv420p",
		"-c:v", "libvpx",
		"-cpu-used", cpuUsed,
		"-deadline", "realtime",
		"-lag-in-frames", "0",
		"-error-resilient", "1",
		"-b:v", vBitrate,
		"-maxrate", vBitrate,
		"-bufsize", vBufsize,
		"-keyint_min", gopStr,
		"-g", gopStr,
		"-auto-alt-ref", "0",
		"-payload_type", "96",
		"-ssrc", "11111111",
		"-f", "rtp",
		fmt.Sprintf("rtp://127.0.0.1:%d", s.videoPort),
	)

	if source != "" {
		aBitrate := envOrDefault("AUDIO_BITRATE", "128k")

		args = append(args,
			"-map", "0:a:0?",
		)

		if audioDelayMs > 0 {
			args = append(args,
				"-af", fmt.Sprintf("adelay=delays=%d:all=1", audioDelayMs),
			)
		}

		args = append(args,
			"-c:a", "libopus",
			"-b:a", aBitrate,
			"-ar", "48000",
			"-ac", "2",
			"-payload_type", "111",
			"-ssrc", "22222222",
			"-f", "rtp",
			fmt.Sprintf("rtp://127.0.0.1:%d", s.audioPort),
		)
	}


	log.Printf("[FFmpeg] Starting: source=%s video=:%d audio=:%d", source, s.videoPort, s.audioPort)

	cmd := exec.Command(getFfmpegPath(), args...)
	cmd.Stdout = nil
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Printf("[FFmpeg] Start error: %v", err)
		return
	}
	s.ffmpeg = cmd

	go func() {
		err := cmd.Wait()
		log.Printf("[FFmpeg] Exited: %v", err)
	}()
}

func (s *Sidecar) StopFFmpegLocked() {
	if s.ffmpeg != nil && s.ffmpeg.Process != nil {
		s.ffmpeg.Process.Kill()
		s.ffmpeg = nil
	}
}

func (s *Sidecar) GetStats() map[string]interface{} {
	s.peersLock.RLock()
	defer s.peersLock.RUnlock()

	peers := map[string]interface{}{}
	for id, peer := range s.peers {
		peers[id] = map[string]interface{}{
			"active": peer.Active,
			"state":  peer.PC.ICEConnectionState().String(),
		}
	}

	return map[string]interface{}{
		"videoPort": s.videoPort,
		"audioPort": s.audioPort,
		"peerCount": len(s.peers),
		"peers":     peers,
		"source":    s.source,
	}
}

func (s *Sidecar) Stop() {
	s.running = false
	s.ffmpegLock.Lock()
	s.StopFFmpegLocked()
	s.ffmpegLock.Unlock()

	if s.videoConn != nil {
		s.videoConn.Close()
	}
	if s.audioConn != nil {
		s.audioConn.Close()
	}

	s.peersLock.Lock()
	for id, peer := range s.peers {
		peer.Close()
		delete(s.peers, id)
	}
	s.peersLock.Unlock()
}

func main() {
	port := 9800
	if p := os.Getenv("SIDECAR_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			port = v
		}
	}

	sidecar := NewSidecar()
	if err := sidecar.StartRTP(); err != nil {
		log.Fatalf("Failed to start RTP: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /peer/create", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		debugf("[API] Peer create requested: %s", req.ID)

		sdp, err := sidecar.CreatePeer(req.ID)
		if err != nil {
			log.Printf("[API] CreatePeer error: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"sdp": sdp})
	})

	mux.HandleFunc("POST /peer/answer", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID  string `json:"id"`
			SDP string `json:"sdp"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		log.Printf("[API] Setting answer for peer: %s (%d bytes)", req.ID, len(req.SDP))

		if err := sidecar.SetAnswer(req.ID, req.SDP); err != nil {
			log.Printf("[API] SetAnswer error: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /peer/ice", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID            string `json:"id"`
			Candidate     string `json:"candidate"`
			SDPMid        string `json:"sdpMid"`
			SDPMLineIndex uint16 `json:"sdpMLineIndex"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		if err := sidecar.AddICECandidate(req.ID, req.Candidate, req.SDPMid, req.SDPMLineIndex); err != nil {
			log.Printf("[API] AddICE error: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /peer/close", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID string `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		sidecar.ClosePeer(req.ID)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /source", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Source    string `json:"source"`
			Width     int    `json:"width"`
			Height    int    `json:"height"`
			Framerate int    `json:"framerate"`
			Bitrate   string `json:"bitrate"` 
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		log.Printf("[API] Setting source: %s (%dx%d @ %dfps, bitrate=%s)", req.Source, req.Width, req.Height, req.Framerate, req.Bitrate)
		sidecar.StartFFmpeg(req.Source, req.Width, req.Height, req.Framerate, req.Bitrate)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /source/stop", func(w http.ResponseWriter, r *http.Request) {
		sidecar.ffmpegLock.Lock()
		sidecar.StopFFmpegLocked()
		sidecar.resetSyncTiming()
		sidecar.drainRTPQueues()
		sidecar.resetPeerStreamState()
		sidecar.ffmpegLock.Unlock()

		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(sidecar.GetStats())
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "ok",
			"videoPort": sidecar.videoPort,
			"audioPort": sidecar.audioPort,
		})
	})

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		sidecar.Stop()
		os.Exit(0)
	}()

	log.Printf("[Sidecar] HTTP API listening on :%d", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}
