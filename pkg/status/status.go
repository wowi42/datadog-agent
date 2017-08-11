package status

import (
	"encoding/json"
	"expvar"
	"os"
	"strconv"
	"time"

	"github.com/DataDog/datadog-agent/pkg/collector/check"
	"github.com/DataDog/datadog-agent/pkg/config"
	"github.com/DataDog/datadog-agent/pkg/metadata/host"
	"github.com/DataDog/datadog-agent/pkg/util"
	"github.com/DataDog/datadog-agent/pkg/version"
	"github.com/DataDog/gohai/platform"
	log "github.com/cihub/seelog"
)

var timeFormat = "2006-01-02 15:04:05.000000 UTC"

// GetStatus grabs the status from expvar and puts it into a map
func GetStatus() (map[string]interface{}, error) {
	stats := make(map[string]interface{})
	stats, err := expvarStats(stats)
	if err != nil {
		log.Errorf("Error Getting ExpVar Stats: %v", err)
	}

	stats["version"] = version.AgentVersion
	hostname, err := util.GetHostname()
	if err != nil {
		log.Errorf("Error grabbing hostname for status: %v", err)
		stats["metadata"] = host.GetPayload("unknown")
	} else {
		stats["metadata"] = host.GetPayload(hostname)
	}

	stats["config"] = getConfig()
	stats["conf_file"] = config.Datadog.ConfigFileUsed()

	platformPayload, err := new(platform.Platform).Collect()
	if err != nil {
		return nil, err
	}
	stats["pid"] = os.Getpid()
	stats["platform"] = platformPayload
	stats["hostinfo"] = host.GetStatusInformation()
	now := time.Now()
	stats["time"] = now.Format(timeFormat)

	return stats, nil
}

// GetAndFormatStatus gets and formats the status all in one go
func GetAndFormatStatus() ([]byte, error) {
	s, err := GetStatus()
	if err != nil {
		return nil, err
	}

	statusJSON, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	st, err := FormatStatus(statusJSON)
	if err != nil {
		return nil, err
	}

	return []byte(st), nil
}

// GetCheckStatus gets the status of a single check
func GetCheckStatus(c check.Check, cs *check.Stats) ([]byte, error) {
	s, err := GetStatus()
	if err != nil {
		return nil, err
	}
	checks := s["runnerStats"].(map[string]interface{})["Checks"]
	checks.(map[string]interface{})[c.String()] = cs

	statusJSON, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	st, err := renderCheckStats(statusJSON, c.String())
	if err != nil {
		return nil, err
	}

	return []byte(st), nil
}

func getConfig() map[string]interface{} {
	var conf = config.Datadog.AllSettings()
	newConf := make(map[string]interface{})
	for k, v := range conf {
		if k != "api_key" && k != "metadata_collectors" {
			newConf[k] = v
		}
	}
	return newConf
}

func expvarStats(stats map[string]interface{}) (map[string]interface{}, error) {
	var err error
	forwarderStatsJSON := []byte(expvar.Get("forwarder").String())
	forwarderStats := make(map[string]interface{})
	json.Unmarshal(forwarderStatsJSON, &forwarderStats)
	stats["forwarderStats"] = forwarderStats

	runnerStatsJSON := []byte(expvar.Get("runner").String())
	runnerStats := make(map[string]interface{})
	json.Unmarshal(runnerStatsJSON, &runnerStats)
	stats["runnerStats"] = runnerStats

	loaderStatsJSON := []byte(expvar.Get("loader").String())
	loaderStats := make(map[string]interface{})
	json.Unmarshal(loaderStatsJSON, &loaderStats)
	stats["loaderStats"] = loaderStats

	aggregatorStatsJSON := []byte(expvar.Get("aggregator").String())
	aggregatorStats := make(map[string]interface{})
	json.Unmarshal(aggregatorStatsJSON, &aggregatorStats)
	stats["aggregatorStats"] = aggregatorStats

	if expvar.Get("ntpOffset").String() != "" {
		stats["ntpOffset"], err = strconv.ParseFloat(expvar.Get("ntpOffset").String(), 64)
	}

	return stats, err
}