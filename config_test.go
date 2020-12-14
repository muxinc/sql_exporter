package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
)

const (
	testPostgresGuageConfigYAML = `
jobs:
- name: "global"
  interval: '5m'
  connections:
  - 'postgres://postgres@localhost/postgres?sslmode=disable'
  startup_sql:
  - 'SET lock_timeout = 1000'
  - 'SET idle_in_transaction_session_timeout = 100'
  queries:
  - name: "running_queries"
    help: "Number of running queries"
    type: "gauge"
    labels:
      - "datname"
      - "usename"
    values:
      - "count"
    query:  |
            SELECT datname::text, usename::text, COUNT(*)::float AS count
            FROM pg_stat_activity GROUP BY datname, usename;
`

	testClickhouseHistogramConfigYAML = `
jobs:
- name: "global"
  interval: '5m'
  connections:
  - 'clickhouse://USERNAME:PASSWORD@localhost:9090/default'
  queries:
  - name: "http_requests_hist"
    help: "HTTP requests histogram buckets"
    type: "histogram"
    labels:
      - "status_code"
    hist_values:
      - name: "http_request_duration_hist"
        count: "http_request_duration_hist_count"
        sum: "http_request_duration_hist_sum"
        buckets:
          - name: "http_request_duration_hist_bucket_b100"
            value: "0.1"
          - name: "http_request_duration_hist_bucket_b500"
            value: "0.5"
          - name: "http_request_duration_hist_bucket_b1000"
            value: "1.0"
          - name: "http_request_duration_hist_bucket_b2500"
            value: "2.5"
          - name: "http_request_duration_hist_bucket_b5000"
            value: "5.0"
    query:  |
            SELECT
              toString(http_code) AS status_code,
              sum(http_request_duration_100ms) AS http_request_duration_hist_bucket_b100,
              sum(http_request_duration_500ms) AS http_request_duration_hist_bucket_b500,
              sum(http_request_duration_1000ms) AS http_request_duration_hist_bucket_b1000,
              sum(http_request_duration_2500ms) AS http_request_duration_hist_bucket_b2500,
              sum(http_request_duration_5000ms) AS http_request_duration_hist_bucket_b5000,
              sum(http_request_duration_ms) AS http_request_duration_hist_sum,
              count(*) AS http_request_duration_hist_count
            FROM http_requests
            GROUP BY
              status_code;
`
)

func Test_parseConfig(t *testing.T) {
	tests := []struct {
		name string
		in   io.Reader
		out  File
	}{
		{
			name: "postgres guage",
			in:   strings.NewReader(testPostgresGuageConfigYAML),
			out: File{
				Jobs: []*Job{
					&Job{
						Name:        "global",
						Interval:    5 * time.Minute,
						Connections: []string{"postgres://postgres@localhost/postgres?sslmode=disable"},
						StartupSQL: []string{
							"SET lock_timeout = 1000",
							"SET idle_in_transaction_session_timeout = 100",
						},
						Queries: []*Query{
							&Query{
								Name:   "running_queries",
								Help:   "Number of running queries",
								Type:   "gauge",
								Labels: []string{"datname", "usename"},
								Values: []string{"count"},
								Query:  "SELECT datname::text, usename::text, COUNT(*)::float AS count\nFROM pg_stat_activity GROUP BY datname, usename;\n",
							},
						},
					},
				},
			},
		},
		{
			name: "clickhouse histogram",
			in:   strings.NewReader(testClickhouseHistogramConfigYAML),
			out: File{
				Jobs: []*Job{
					&Job{
						Name:        "global",
						Interval:    5 * time.Minute,
						Connections: []string{"clickhouse://USERNAME:PASSWORD@localhost:9090/default"},
						Queries: []*Query{
							&Query{
								Name: "http_requests_hist",
								Help: "HTTP requests histogram buckets",
								Type: "histogram",
								Labels: []string{
									"status_code",
								},
								HistValues: []*HistValue{
									&HistValue{
										Count: "http_request_duration_hist_count",
										Sum:   "http_request_duration_hist_sum",
										Buckets: []*Bucket{
											&Bucket{Name: "http_request_duration_hist_bucket_b100", Value: "0.1"},
											&Bucket{Name: "http_request_duration_hist_bucket_b500", Value: "0.5"},
											&Bucket{Name: "http_request_duration_hist_bucket_b1000", Value: "1.0"},
											&Bucket{Name: "http_request_duration_hist_bucket_b2500", Value: "2.5"},
											&Bucket{Name: "http_request_duration_hist_bucket_b5000", Value: "5.0"},
										},
									},
								},
								Query: "SELECT\n  toString(http_code) AS status_code,\n  sum(http_request_duration_100ms) AS http_request_duration_hist_bucket_b100,\n  sum(http_request_duration_500ms) AS http_request_duration_hist_bucket_b500,\n  sum(http_request_duration_1000ms) AS http_request_duration_hist_bucket_b1000,\n  sum(http_request_duration_2500ms) AS http_request_duration_hist_bucket_b2500,\n  sum(http_request_duration_5000ms) AS http_request_duration_hist_bucket_b5000,\n  sum(http_request_duration_ms) AS http_request_duration_hist_sum,\n  count(*) AS http_request_duration_hist_count\nFROM http_requests\nGROUP BY\n  status_code;\n",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConfig(tt.in)
			if err != nil {
				t.Errorf("got unexpected error: %v", err)
			} else if diff := pretty.Compare(tt.out, got); diff != "" {
				t.Errorf("expected response was not as expected (-have +want):\n\n%s", diff)
			}
		})
	}
}
