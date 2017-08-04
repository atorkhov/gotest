package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

var myClient = &http.Client{Timeout: 10 * time.Second}

type TableData struct {
	Table TableContainer `json:"table"`
}

type TableContainer struct {
	Rows []TableRow `json:"rows"`
}

type TableRow struct {
	Station            string
	MooringSiteDesc    string
	WaterDepth         float64
	Time               string
	CurrentSpeed       float64
	CurrentSpeedQC     int8
	CurrentDirection   float64
	CurrentDirectionQC int8
	CurrentU           float64
	CurrentUQC         int8
	CurrentV           float64
	CurrentVQC         int8
	Temperature        float64
	TemperatureQC      int8
	Conductivity       float64
	ConductivityQC     int8
	Salinity           float64
	SalinityQC         int8
	SigmaT             float64
	SigmaTQC           int8
	TimeCreated        string
	TimeModified       string
	Longitude          float64
	Latitude           float64
	Depth              float64
}

func (r *TableRow) UnmarshalJSON(b []byte) error {
	var rawData []interface{}
	if err := json.Unmarshal(b, &rawData); err != nil {
		return err
	}

	r.Station = rawData[0].(string)
	r.MooringSiteDesc = rawData[1].(string)
	r.WaterDepth = rawData[2].(float64)
	r.Time = rawData[3].(string)
	r.CurrentSpeed = rawData[4].(float64)
	r.CurrentSpeedQC = int8(rawData[5].(float64))
	r.CurrentDirection = rawData[6].(float64)
	r.CurrentDirectionQC = int8(rawData[7].(float64))
	r.CurrentU = rawData[8].(float64)
	r.CurrentUQC = int8(rawData[9].(float64))
	r.CurrentV = rawData[10].(float64)
	r.CurrentVQC = int8(rawData[11].(float64))
	r.Temperature = rawData[12].(float64)
	r.TemperatureQC = int8(rawData[13].(float64))
	r.Conductivity = rawData[14].(float64)
	r.ConductivityQC = int8(rawData[15].(float64))
	r.Salinity = rawData[16].(float64)
	r.SalinityQC = int8(rawData[17].(float64))
	r.SigmaT = rawData[18].(float64)
	r.SigmaTQC = int8(rawData[19].(float64))
	r.TimeCreated = rawData[20].(string)
	r.TimeModified = rawData[21].(string)
	r.Longitude = rawData[22].(float64)
	r.Latitude = rawData[23].(float64)
	r.Depth = rawData[24].(float64)

	return nil
}

type Result struct {
	CurrentSpeed AggregateCurrentSpeed
	Salinity     AggregateSalinity
	Temperature  AggregateTemperature
}

type AggregateResult struct {
	MinDate    string
	MaxDate    string
	NumRecords int32
	MinValue   float64
	MaxValue   float64
	AvgValue   float64
}

func (agg *AggregateResult) update(time string, value float64, qc int8) {
	if qc != 0 {
		return
	}

	if agg.MinDate == "" {
		agg.MinDate = time[0:10]
	}
	agg.MaxDate = time[0:10]

	if agg.MinValue == 0 || agg.MinValue > value {
		agg.MinValue = value
	}

	if agg.MaxValue == 0 || agg.MaxValue < value {
		agg.MaxValue = value
	}

	agg.AvgValue = (agg.AvgValue*float64(agg.NumRecords) + value) / float64(agg.NumRecords+1)
	agg.NumRecords += 1
}

type AggregateCurrentSpeed struct {
	AggregateResult
}

func (agg *AggregateCurrentSpeed) GetNamed() interface{} {
	return &struct {
		MinDate    string  `json:"min_date"`
		MaxDate    string  `json:"max_date"`
		NumRecords int32   `json:"num_records"`
		MinValue   float64 `json:"min_current_speed"`
		MaxValue   float64 `json:"max_current_speed"`
		AvgValue   float64 `json:"avg_current_speed"`
	}{
		MinDate:    agg.MinDate,
		MaxDate:    agg.MaxDate,
		NumRecords: agg.NumRecords,
		MinValue:   agg.MinValue,
		MaxValue:   agg.MaxValue,
		AvgValue:   agg.AvgValue,
	}
}

type AggregateSalinity struct {
	AggregateResult
}

func (agg *AggregateSalinity) GetNamed() interface{} {
	return &struct {
		MinDate    string  `json:"min_date"`
		MaxDate    string  `json:"max_date"`
		NumRecords int32   `json:"num_records"`
		MinValue   float64 `json:"min_salinity"`
		MaxValue   float64 `json:"max_salinity"`
		AvgValue   float64 `json:"avg_salinity"`
	}{
		MinDate:    agg.MinDate,
		MaxDate:    agg.MaxDate,
		NumRecords: agg.NumRecords,
		MinValue:   agg.MinValue,
		MaxValue:   agg.MaxValue,
		AvgValue:   agg.AvgValue,
	}
}

type AggregateTemperature struct {
	AggregateResult
}

func (agg *AggregateTemperature) GetNamed() interface{} {
	return &struct {
		MinDate    string  `json:"min_date"`
		MaxDate    string  `json:"max_date"`
		NumRecords int32   `json:"num_records"`
		MinValue   float64 `json:"min_temperature"`
		MaxValue   float64 `json:"max_temperature"`
		AvgValue   float64 `json:"avg_temperature"`
	}{
		MinDate:    agg.MinDate,
		MaxDate:    agg.MaxDate,
		NumRecords: agg.NumRecords,
		MinValue:   agg.MinValue,
		MaxValue:   agg.MaxValue,
		AvgValue:   agg.AvgValue,
	}
}

func (res *Result) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		CurrentSpeed interface{} `json:"current_speed"`
		Salinity     interface{} `json:"salinity"`
		Temperature  interface{} `json:"temperature"`
	}{
		CurrentSpeed: res.CurrentSpeed.GetNamed(),
		Salinity:     res.Salinity.GetNamed(),
		Temperature:  res.Temperature.GetNamed(),
	})
}

func getJson(url string, target interface{}) error {
	r, err := myClient.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

func main() {
	url := "http://www.neracoos.org/erddap/tabledap/E05_aanderaa_all.json?station%2Cmooring_site_desc%2Cwater_depth%2Ctime%2Ccurrent_speed%2Ccurrent_speed_qc%2Ccurrent_direction%2Ccurrent_direction_qc%2Ccurrent_u%2Ccurrent_u_qc%2Ccurrent_v%2Ccurrent_v_qc%2Ctemperature%2Ctemperature_qc%2Cconductivity%2Cconductivity_qc%2Csalinity%2Csalinity_qc%2Csigma_t%2Csigma_t_qc%2Ctime_created%2Ctime_modified%2Clongitude%2Clatitude%2Cdepth&time%3E=2015-08-25T15%3A00%3A00Z&time%3C=2016-12-05T14%3A00%3A00Z"

	foo := TableData{}
	result := Result{}

	getJson(url, &foo)

	for i := 0; i < len(foo.Table.Rows); i++ {
		row := &foo.Table.Rows[i]

		result.CurrentSpeed.update(row.Time, row.CurrentSpeed, row.CurrentSpeedQC)
		result.Salinity.update(row.Time, row.Salinity, row.SalinityQC)
		result.Temperature.update(row.Time, row.Temperature, row.TemperatureQC)
	}

	jsonResult, err := json.Marshal(&result)
	if err != nil {
		return
	}

	fmt.Println(string(jsonResult))
}
