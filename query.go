package main

import (
	"fmt"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricTypeGauge = "gauge"
	metricTypeHist  = "histogram"
)

// Run executes a single Query on a single connection
func (q *Query) Run(conn *connection) error {
	if q.log == nil {
		q.log = log.NewNopLogger()
	}
	if q.desc == nil {
		return fmt.Errorf("metrics descriptor is nil")
	}
	if q.Query == "" {
		return fmt.Errorf("query is empty")
	}
	if conn == nil || conn.conn == nil {
		return fmt.Errorf("db connection not initialized (should not happen)")
	}
	// execute query
	rows, err := conn.conn.Queryx(q.Query)
	if err != nil {
		failedScrapes.WithLabelValues(conn.driver, conn.host, conn.database, conn.user, q.jobName, q.Name).Set(1.0)
		return err
	}
	defer rows.Close()

	updated := 0
	metrics := make([]prometheus.Metric, 0, len(q.metrics))
	for rows.Next() {
		res := make(map[string]interface{})
		err := rows.MapScan(res)
		if err != nil {
			level.Error(q.log).Log("msg", "Failed to scan", "err", err, "host", conn.host, "db", conn.database)
			failedScrapes.WithLabelValues(conn.driver, conn.host, conn.database, conn.user, q.jobName, q.Name).Set(1.0)
			continue
		}
		var m []prometheus.Metric
		switch q.Type {
		case metricTypeGauge:
			m, err = q.updateConstMetrics(conn, res)
		case metricTypeHist:
			m, err = q.updateHistMetrics(conn, res)
		default:
			// backward compatible: default to const gauge metric
			m, err = q.updateConstMetrics(conn, res)
		}
		if err != nil {
			level.Error(q.log).Log("msg", "Failed to update metrics", "err", err, "host", conn.host, "db", conn.database)
			failedScrapes.WithLabelValues(conn.driver, conn.host, conn.database, conn.user, q.jobName, q.Name).Set(1.0)
			continue
		}
		metrics = append(metrics, m...)
		updated++
		failedScrapes.WithLabelValues(conn.driver, conn.host, conn.database, conn.user, q.jobName, q.Name).Set(0.0)
	}

	if updated < 1 {
		return fmt.Errorf("zero rows returned")
	}

	// update the metrics cache
	q.Lock()
	q.metrics[conn] = metrics
	q.Unlock()

	return nil
}

// updateConstMetrics parses the result set and returns a slice of const metrics.
func (q *Query) updateConstMetrics(conn *connection, res map[string]interface{}) ([]prometheus.Metric, error) {
	updated := 0
	metrics := make([]prometheus.Metric, 0, len(q.Values))
	for _, valueName := range q.Values {
		m, err := q.updateConstMetric(conn, res, valueName)
		if err != nil {
			level.Error(q.log).Log(
				"msg", "Failed to update metric",
				"value", valueName,
				"err", err,
				"host", conn.host,
				"db", conn.database,
			)
			continue
		}
		metrics = append(metrics, m)
		updated++
	}
	if updated < 1 {
		return nil, fmt.Errorf("zero values found")
	}
	return metrics, nil
}

// updateHistMetrics parses the result set and returns a slice of histogram metrics.
func (q *Query) updateHistMetrics(conn *connection, res map[string]interface{}) ([]prometheus.Metric, error) {
	updated := 0
	metrics := make([]prometheus.Metric, 0, len(q.Values))
	for _, histValue := range q.HistValues {
		m, err := q.updateHistogramMetric(conn, res, histValue)
		if err != nil {
			level.Error(q.log).Log(
				"msg", "Failed to update metric",
				"value", histValue.Name,
				"err", err,
				"host", conn.host,
				"db", conn.database,
			)
			continue
		}
		metrics = append(metrics, m)
		updated++
	}
	if updated < 1 {
		return nil, fmt.Errorf("zero values found")
	}
	return metrics, nil
}

func parseValue(res map[string]interface{}, valueName string) (float64, error) {
	var value float64
	if i, ok := res[valueName]; ok {
		switch f := i.(type) {
		case int:
			value = float64(f)
		case int32:
			value = float64(f)
		case int64:
			value = float64(f)
		case uint:
			value = float64(f)
		case uint32:
			value = float64(f)
		case uint64:
			value = float64(f)
		case float32:
			value = float64(f)
		case float64:
			value = float64(f)
		case []uint8:
			val, err := strconv.ParseFloat(string(f), 64)
			if err != nil {
				return 0.0, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
			}
			value = val
		case string:
			val, err := strconv.ParseFloat(f, 64)
			if err != nil {
				return 0.0, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
			}
			value = val
		default:
			return 0.0, fmt.Errorf("Column '%s' must be type float, is '%T' (val: %s)", valueName, i, f)
		}
	}
	return value, nil
}

func buildLabels(conn *connection, res map[string]interface{}, valueName string, inLabels []string) ([]string, error) {
	// make space for all defined variable label columns and the "static" labels
	// added below
	labels := make([]string, 0, len(inLabels)+5)
	for _, label := range inLabels {
		// we need to fill every spot in the slice or the key->value mapping
		// won't match up in the end.
		//
		// ORDER MATTERS!
		lv := ""
		if i, ok := res[label]; ok {
			switch str := i.(type) {
			case string:
				lv = str
			case []uint8:
				lv = string(str)
			default:
				return nil, fmt.Errorf("Column '%s' must be type text (string)", label)
			}
		}
		labels = append(labels, lv)
	}
	labels = append(labels, conn.driver)
	labels = append(labels, conn.host)
	labels = append(labels, conn.database)
	labels = append(labels, conn.user)
	labels = append(labels, valueName)
	return labels, nil
}

// updateMetrics parses a single row and returns a const metric.
func (q *Query) updateConstMetric(conn *connection, res map[string]interface{}, valueName string) (prometheus.Metric, error) {
	// parse value from result
	value, err := parseValue(res, valueName)
	if err != nil {
		return nil, err
	}

	// build user defined labels along with pre-defined "static" labels
	labels, err := buildLabels(conn, res, valueName, q.Labels)
	if err != nil {
		return nil, err
	}

	// create a new immutable const metric that can be cached and returned on
	// every scrape. Remember that the order of the lable values in the labels
	// slice must match the order of the label names in the descriptor!
	return prometheus.NewConstMetric(q.desc, prometheus.GaugeValue, value, labels...)
}

// updateHistogramMetric parses rows to return a histogram metric.
func (q *Query) updateHistogramMetric(conn *connection, res map[string]interface{}, histValue *HistValue) (prometheus.Metric, error) {
	// parse hist count
	countValue, err := parseValue(res, histValue.Count)
	if err != nil {
		return nil, err
	}

	// parse hist sum
	sumVal, err := parseValue(res, histValue.Sum)
	if err != nil {
		return nil, err
	}

	// parse hist buckets
	bucketVals := make(map[float64]uint64, len(histValue.Buckets))
	for _, bucket := range histValue.Buckets {
		b, err := strconv.ParseFloat(bucket.Value, 64)
		if err != nil {
			return nil, err
		}
		bVal, err := parseValue(res, bucket.Name)
		if err != nil {
			return nil, err
		}
		bucketVals[b] = uint64(bVal)
	}

	// build user defined labels along with pre-defined "static" labels
	labels, err := buildLabels(conn, res, histValue.Name, q.Labels)
	if err != nil {
		return nil, err
	}

	// create a new immutable const histogram that can be cached and returned on
	// every scrape
	return prometheus.NewConstHistogram(q.desc, uint64(countValue), sumVal, bucketVals, labels...)
}
