package diagnostic

import (
	"fmt"
	"net"
	"time"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/uber-go/zap"
)

type CollectdHandler struct {
	l zap.Logger
}

func (s *Service) InCollectdContext() collectd.Diagnostic {
	if s == nil {
		return nil
	}
	return &CollectdHandler{l: s.l.With(zap.String("service", "collectd"))}
}

func (h *CollectdHandler) Starting() {
	h.l.Info("Starting collectd service")
}

func (h *CollectdHandler) Listening(addr net.Addr) {
	h.l.Info(fmt.Sprint("Listening on UDP: ", addr.String()))
}

func (h *CollectdHandler) Closed() {
	h.l.Info("collectd UDP closed")
}

func (h *CollectdHandler) UnableToReadDirectory(path string, err error) {
	h.l.Info(fmt.Sprintf("Unable to read directory %s: %s\n", path, err))
}

func (h *CollectdHandler) LoadingPath(path string) {
	h.l.Info(fmt.Sprintf("Loading %s\n", path))
}

func (h *CollectdHandler) TypesParseError(path string, err error) {
	h.l.Info(fmt.Sprintf("Unable to parse collectd types file: %s\n", path))
}

func (h *CollectdHandler) ReadFromUDPError(err error) {
	h.l.Info(fmt.Sprintf("collectd ReadFromUDP error: %s", err))
}

func (h *CollectdHandler) ParseError(err error) {
	h.l.Info(fmt.Sprintf("Collectd parse error: %s", err))
}

func (h *CollectdHandler) DroppingPoint(name string, err error) {
	h.l.Info(fmt.Sprintf("Dropping point %v: %v", name, err))
}

func (h *CollectdHandler) InternalStorageCreateError(err error) {
	h.l.Info(fmt.Sprintf("Required database or retention policy do not yet exist: %s", err.Error()))
}

func (h *CollectdHandler) PointWriterError(database string, err error) {
	h.l.Info(fmt.Sprintf("failed to write point batch to database %q: %s", database, err))
}

type ContinuousQuerierHandler struct {
	l zap.Logger
}

func (s *Service) InContinuousQuerierContext() continuous_querier.Diagnostic {
	if s == nil {
		return nil
	}
	return &ContinuousQuerierHandler{l: s.l.With(zap.String("service", "continuous_querier"))}
}

func (h *ContinuousQuerierHandler) Starting() {
	h.l.Info("Starting continuous query service")
}

func (h *ContinuousQuerierHandler) Closing() {
	h.l.Info("continuous query service terminating")
}

func (h *ContinuousQuerierHandler) RunningByRequest(now time.Time) {
	h.l.Info(fmt.Sprintf("running continuous queries by request for time: %v", now))
}

func (h *ContinuousQuerierHandler) ExecuteContinuousQuery(name string, start, end time.Time) {
	h.l.Info(fmt.Sprintf("executing continuous query %s (%v to %v)", name, start, end))
}

func (h *ContinuousQuerierHandler) ExecuteContinuousQueryError(query string, err error) {
	h.l.Info(fmt.Sprintf("error executing query: %s: err = %s", query, err))
}

func (h *ContinuousQuerierHandler) FinishContinuousQuery(name string, written int64, start, end time.Time, dur time.Duration) {
	h.l.Info(fmt.Sprintf("finished continuous query %s, %d points(s) written (%v to %v) in %s", name, written, start, end, dur))
}

type GraphiteHandler struct {
	l zap.Logger
}

func (s *Service) InGraphiteContext() graphite.Diagnostic {
	if s == nil {
		return nil
	}
	return &GraphiteHandler{l: s.l.With(zap.String("service", "graphite"))}
}

func (h *GraphiteHandler) WithContext(bindAddress string) graphite.Diagnostic {
	return &GraphiteHandler{l: h.l.With(zap.String("addr", bindAddress))}
}

func (h *GraphiteHandler) Starting(batchSize int, batchTimeout time.Duration) {
	h.l.Info("starting graphite service", zap.Int("batchSize", batchSize), zap.String("batchTimeout", batchTimeout.String()))
}

func (h *GraphiteHandler) Listening(protocol string, addr net.Addr) {
	h.l.Info("listening", zap.String("protocol", protocol), zap.String("addr", addr.String()))
}

func (h *GraphiteHandler) TCPListenerClosed() {
	h.l.Info("graphite TCP listener closed")
}

func (h *GraphiteHandler) TCPAcceptError(err error) {
	h.l.Info("error accepting TCP connection", zap.Error(err))
}

func (h *GraphiteHandler) LineParseError(line string, err error) {
	h.l.Info(fmt.Sprintf("unable to parse line: %s: %s", line, err))
}

func (h *GraphiteHandler) InternalStorageCreateError(err error) {
	h.l.Info(fmt.Sprintf("Required database or retention policy do not yet exist: %s", err.Error()))
}

func (h *GraphiteHandler) PointWriterError(database string, err error) {
	h.l.Info(fmt.Sprintf("failed to write point batch to database %q: %s", database, err))
}

type HTTPDHandler struct {
	l zap.Logger
}

func (s *Service) InHTTPDContext() httpd.Diagnostic {
	if s == nil {
		return nil
	}
	return &HTTPDHandler{l: s.l.With(zap.String("service", "httpd"))}
}

func (h *HTTPDHandler) Starting(authEnabled bool) {
	h.l.Info("Starting HTTP service")
	h.l.Info(fmt.Sprint("Authentication enabled:", authEnabled))
}

func (h *HTTPDHandler) Listening(protocol string, addr net.Addr) {
	h.l.Info(fmt.Sprintf("Listening on %s: %s", protocol, addr.String()))
}

func (h *HTTPDHandler) UnauthorizedRequest(user meta.User, query, database string) {
	h.l.Info(fmt.Sprintf("Unauthorized request | user: %q | query: %q | database %q", user, query, database))
}

func (h *HTTPDHandler) AsyncQueryError(query string, err error) {
	h.l.Info(fmt.Sprintf("error while running async query: %s: %s", query, err))
}

func (h *HTTPDHandler) WriteBodyReceived(bytes []byte) {
	h.l.Info(fmt.Sprintf("Write body received by handler: %s", bytes))
}

func (h *HTTPDHandler) WriteBodyReadError() {
	h.l.Info("Write handler unable to read bytes from request body")
}

func (h *HTTPDHandler) StatusDeprecated() {
	h.l.Info("WARNING: /status has been deprecated.  Use /ping instead.")
}

func (h *HTTPDHandler) PromWriteBodyReceived(body []byte) {
	h.l.Info(fmt.Sprintf("Prom write body received by handler: %s", body))
}

func (h *HTTPDHandler) PromWriteBodyReadError() {
	h.l.Info("Prom write handler unable to read bytes from request body")
}

func (h *HTTPDHandler) PromWriteError(err error) {
	h.l.Info(fmt.Sprintf("Prom write handler: %s", err.Error()))
}

func (h *HTTPDHandler) JWTClaimsAssertError() {
	h.l.Info("Could not assert JWT token claims as jwt.MapClaims")
}

func (h *HTTPDHandler) HttpError(status int, errStr string) {
	h.l.Error(fmt.Sprintf("[%d] - %q", status, errStr))
}

type MetaClientHandler struct {
	l zap.Logger
}

func (s *Service) InMetaClientContext() meta.Diagnostic {
	if s == nil {
		return nil
	}
	return &MetaClientHandler{l: s.l.With(zap.String("service", "metaclient"))}
}

func (h *MetaClientHandler) ShardGroupExists(group uint64, db, rp string) {
	h.l.Info(fmt.Sprintf("shard group %d exists for database %s, retention policy %s", group, db, rp))
}

func (h *MetaClientHandler) PrecreateShardError(group uint64, err error) {
	h.l.Info(fmt.Sprintf("failed to precreate successive shard group for group %d: %s", group, err))
}

func (h *MetaClientHandler) NewShardGroup(group uint64, db, rp string) {
	h.l.Info(fmt.Sprintf("new shard group %d successfully precreated for database %s, retention policy %s", group, db, rp))
}

type MonitorHandler struct {
	l zap.Logger
}

func (s *Service) InMonitorContext() monitor.Diagnostic {
	if s == nil {
		return nil
	}
	return &MonitorHandler{l: s.l.With(zap.String("service", "monitor"))}
}

func (h *MonitorHandler) Starting() {
	h.l.Info("Starting monitor system")
}

func (h *MonitorHandler) AlreadyOpen() {
	h.l.Info("Monitor is already open")
}

func (h *MonitorHandler) Closing() {
	h.l.Info("shutting down monitor system")
}

func (h *MonitorHandler) AlreadyClosed() {
	h.l.Info("Monitor is already closed.")
}

func (h *MonitorHandler) DiagnosticRegistered(name string) {
	h.l.Info("registered monitoring diagnostic", zap.String("name", name))
}

func (h *MonitorHandler) CreateInternalStorageFailure(db string, err error) {
	h.l.Info(fmt.Sprintf("failed to create database '%s', failed to create storage: %s",
		db, err.Error()))
}

func (h *MonitorHandler) StoreStatistics(db, rp string, interval time.Duration) {
	h.l.Info(fmt.Sprintf("Storing statistics in database '%s' retention policy '%s', at interval %s",
		db, rp, interval))
}

func (h *MonitorHandler) StoreStatisticsError(err error) {
	h.l.Info(fmt.Sprintf("failed to store statistics: %s", err))
}

func (h *MonitorHandler) StatisticsRetrievalFailure(err error) {
	h.l.Info(fmt.Sprintf("failed to retrieve registered statistics: %s", err))
}

func (h *MonitorHandler) DroppingPoint(name string, err error) {
	h.l.Info(fmt.Sprintf("Dropping point %v: %v", name, err))
}

func (h *MonitorHandler) StoreStatisticsDone() {
	h.l.Info(fmt.Sprintf("terminating storage of statistics"))
}

type OpenTSDBHandler struct {
	l zap.Logger
}

func (s *Service) InOpenTSDBContext() opentsdb.Diagnostic {
	if s == nil {
		return nil
	}
	return &OpenTSDBHandler{l: s.l.With(zap.String("service", "opentsdb"))}
}

func (h *OpenTSDBHandler) Starting() {
	h.l.Info("Starting OpenTSDB service")
}

func (h *OpenTSDBHandler) Listening(tls bool, addr net.Addr) {
	if tls {
		h.l.Info(fmt.Sprint("Listening on TLS: ", addr.String()))
	} else {
		h.l.Info(fmt.Sprint("Listening on: ", addr.String()))
	}
}

func (h *OpenTSDBHandler) Closed(err error) {
	if err != nil {
		h.l.Info(fmt.Sprint("error accepting openTSDB: ", err.Error()))
	} else {
		h.l.Info("openTSDB TCP listener closed")
	}
}

func (h *OpenTSDBHandler) ConnReadError(err error) {
	h.l.Info(fmt.Sprint("error reading from openTSDB connection ", err.Error()))
}

func (h *OpenTSDBHandler) InternalStorageCreateError(database string, err error) {
	h.l.Info(fmt.Sprintf("Required database %s does not yet exist: %s", database, err.Error()))
}

func (h *OpenTSDBHandler) PointWriterError(database string, err error) {
	h.l.Info(fmt.Sprintf("failed to write point batch to database %q: %s", database, err))
}

func (h *OpenTSDBHandler) MalformedLine(line string, addr net.Addr) {
	h.l.Info(fmt.Sprintf("malformed line '%s' from %s", line, addr.String()))
}

func (h *OpenTSDBHandler) MalformedTime(time string, addr net.Addr, err error) {
	h.l.Info(fmt.Sprintf("malformed time '%s' from %s: %s", time, addr.String(), err))
}

func (h *OpenTSDBHandler) MalformedTag(tag string, addr net.Addr) {
	h.l.Info(fmt.Sprintf("malformed tag data '%v' from %s", tag, addr.String()))
}

func (h *OpenTSDBHandler) MalformedFloat(valueStr string, addr net.Addr) {
	h.l.Info(fmt.Sprintf("bad float '%s' from %s", valueStr, addr.String()))
}

func (h *OpenTSDBHandler) DroppingPoint(metric string, err error) {
	h.l.Info(fmt.Sprintf("Dropping point %v: %v", metric, err))
}

func (h *OpenTSDBHandler) WriteError(err error) {
	h.l.Info(fmt.Sprint("write series error: ", err))
}

type PointsWriterHandler struct {
	l zap.Logger
}

func (s *Service) InPointsWriterContext() coordinator.Diagnostic {
	if s == nil {
		return nil
	}
	return &PointsWriterHandler{l: s.l.With(zap.String("service", "write"))}
}

func (h *PointsWriterHandler) WriteFailed(shardID uint64, err error) {
	h.l.Info(fmt.Sprintf("write failed for shard %d: %v", shardID, err))
}

type PrecreatorHandler struct {
	l zap.Logger
}

func (s *Service) InPrecreatorContext() precreator.Diagnostic {
	if s == nil {
		return nil
	}
	return &PrecreatorHandler{l: s.l.With(zap.String("service", "shard-precreation"))}
}

func (h *PrecreatorHandler) Starting(checkInterval, advancePeriod time.Duration) {
	h.l.Info(fmt.Sprintf("Starting precreation service with check interval of %s, advance period of %s",
		checkInterval, advancePeriod))
}

func (h *PrecreatorHandler) Closing() {
	h.l.Info("Precreation service terminating")
}

func (h *PrecreatorHandler) PrecreateError(err error) {
	h.l.Info(fmt.Sprintf("failed to precreate shards: %s", err.Error()))
}

type QueryExecutorHandler struct {
	l zap.Logger
}

func (s *Service) InQueryContext() query.Diagnostic {
	if s == nil {
		return nil
	}
	return &QueryExecutorHandler{l: s.l.With(zap.String("service", "query"))}
}

func (h *QueryExecutorHandler) PrintStatement(query string) {
	h.l.Info(query)
}

func (h *QueryExecutorHandler) QueryPanic(query string, err interface{}, stack []byte) {
	h.l.Error(fmt.Sprintf("%s [panic:%s] %s", query, err, stack))
}

func (h *QueryExecutorHandler) DetectedSlowQuery(query string, qid uint64, database string, threshold time.Duration) {
	h.l.Warn("Detected slow query",
		zap.String("query", query),
		zap.Uint64("qid", qid),
		zap.String("database", database),
		zap.String("threshold", threshold.String()),
	)
}

type RetentionHandler struct {
	l zap.Logger
}

func (s *Service) InRetentionContext() retention.Diagnostic {
	if s == nil {
		return nil
	}
	return &RetentionHandler{l: s.l.With(zap.String("service", "retention"))}
}

func (h *RetentionHandler) Starting(checkInterval time.Duration) {
	h.l.Info(fmt.Sprint("Starting retention policy enforcement service with check interval of ", checkInterval))
}

func (h *RetentionHandler) Closing() {
	h.l.Info("retention policy enforcement terminating")
}

func (h *RetentionHandler) StartingCheck() {
	h.l.Info("retention policy shard deletion check commencing")
}

func (h *RetentionHandler) DeletedShard(id uint64, db, rp string) {
	h.l.Info(fmt.Sprintf("shard ID %d from database %s, retention policy %s, deleted",
		id, db, rp))
}

func (h *RetentionHandler) DeleteShardError(id uint64, db, rp string, err error) {
	h.l.Error(fmt.Sprintf("failed to delete shard ID %d from database %s, retention policy %s: %s",
		id, db, rp, err.Error()))
}

func (h *RetentionHandler) DeletedShardGroup(id uint64, db, rp string) {
	h.l.Info(fmt.Sprintf("deleted shard group %d from database %s, retention policy %s",
		id, db, rp))
}

func (h *RetentionHandler) DeleteShardGroupError(id uint64, db, rp string, err error) {
	h.l.Info(fmt.Sprintf("failed to delete shard group %d from database %s, retention policy %s: %s",
		id, db, rp, err))
}

func (h *RetentionHandler) PruneShardGroupsError(err error) {
	h.l.Info(fmt.Sprintf("error pruning shard groups: %s", err))
}

type SnapshotterHandler struct {
	l zap.Logger
}

func (s *Service) InSnapshotterContext() snapshotter.Diagnostic {
	if s == nil {
		return nil
	}
	return &SnapshotterHandler{l: s.l.With(zap.String("service", "snapshot"))}
}

func (h *SnapshotterHandler) Starting() {
	h.l.Info("Starting snapshot service")
}

func (h *SnapshotterHandler) Closed() {
	h.l.Info("snapshot listener closed")
}

func (h *SnapshotterHandler) AcceptError(err error) {
	h.l.Info(fmt.Sprint("error accepting snapshot request: ", err.Error()))
}

func (h *SnapshotterHandler) Error(err error) {
	h.l.Info(err.Error())
}

type SubscriberHandler struct {
	l zap.Logger
}

func (s *Service) InSubscriberContext() subscriber.Diagnostic {
	if s == nil {
		return nil
	}
	return &SubscriberHandler{l: s.l.With(zap.String("service", "subscriber"))}
}

func (h *SubscriberHandler) Opened() {
	h.l.Info("opened service")
}

func (h *SubscriberHandler) Closed() {
	h.l.Info("closed service")
}

func (h *SubscriberHandler) UpdateSubscriptionError(err error) {
	h.l.Info(fmt.Sprint("error updating subscriptions: ", err))
}

func (h *SubscriberHandler) SubscriptionCreateError(name string, err error) {
	h.l.Info(fmt.Sprintf("Subscription creation failed for '%s' with error: %s", name, err))
}

func (h *SubscriberHandler) AddedSubscription(db, rp string) {
	h.l.Info(fmt.Sprintf("added new subscription for %s %s", db, rp))
}

func (h *SubscriberHandler) DeletedSubscription(db, rp string) {
	h.l.Info(fmt.Sprintf("deleted old subscription for %s %s", db, rp))
}

func (h *SubscriberHandler) SkipInsecureVerify() {
	h.l.Info("WARNING: 'insecure-skip-verify' is true. This will skip all certificate verifications.")
}

func (h *SubscriberHandler) Error(err error) {
	h.l.Info(err.Error())
}

type UDPHandler struct {
	l zap.Logger
}

func (s *Service) InUDPContext() udp.Diagnostic {
	if s == nil {
		return nil
	}
	return &UDPHandler{l: s.l.With(zap.String("service", "udp"))}
}

func (h *UDPHandler) Started(bindAddress string) {
	h.l.Info(fmt.Sprintf("Started listening on UDP: %s", bindAddress))
}

func (h *UDPHandler) Closed() {
	h.l.Info("Service closed")
}

func (h *UDPHandler) CreateInternalStorageFailure(db string, err error) {
	h.l.Info(fmt.Sprintf("Required database %s does not yet exist: %s", db, err.Error()))
}

func (h *UDPHandler) PointWriterError(database string, err error) {
	h.l.Info(fmt.Sprintf("failed to write point batch to database %q: %s", database, err))
}

func (h *UDPHandler) ParseError(err error) {
	h.l.Info(fmt.Sprintf("Failed to parse points: %s", err))
}

func (h *UDPHandler) ReadFromError(err error) {
	h.l.Info(fmt.Sprintf("Failed to read UDP message: %s", err))
}
