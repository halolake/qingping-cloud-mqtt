package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func main() {
	appKey := os.Getenv("APP_KEY")
	appSecret := os.Getenv("APP_SECRET")

	mqttHost := os.Getenv("MQTT_HOST")
	mqttPort, _ := strconv.ParseInt(os.Getenv("MQTT_PORT"), 10, 64)
	mqttUsername := os.Getenv("MQTT_USERNAME")
	mqttPassword := os.Getenv("MQTT_PASSWORD")

	c := cron.New()
	_, err := c.AddFunc("* * * * *", func() {
		logrus.Info("fetch data begin")
		accessToken, err := accessToken(appKey, appSecret)
		if err != nil {
			logrus.WithError(err).Error("fetch access token error")
			return
		}
		logrus.Infof("access token:%v", accessToken)
		deviceDataMap, err := deviceData(accessToken)
		if err != nil {
			logrus.WithError(err).Error("fetch device data error")
			return
		}
		logrus.Infof("device data:%+v", deviceDataMap)
		opts := mqtt.NewClientOptions()
		opts.AddBroker(fmt.Sprintf("tcp://%s:%d", mqttHost, mqttPort))
		opts.SetClientID(fmt.Sprintf("qcm-%s", uuid.New().String()))
		opts.SetUsername(mqttUsername)
		opts.SetPassword(mqttPassword)
		client := mqtt.NewClient(opts)
		connectToken := client.Connect()
		connectToken.Wait()
		err = connectToken.Error()
		if err != nil {
			logrus.WithError(err).Error("mqtt connect error")
			return
		}
		logrus.Info("mqtt connect done")
		for mac, deviceData := range deviceDataMap {
			data, err := json.Marshal(deviceData)
			if err != nil {
				logrus.WithError(err).Error("payload json encode error")
				continue
			}
			publishToken := client.Publish(fmt.Sprintf("qingping/data/%v", mac), 1, false, data)
			publishToken.Wait()
			err = publishToken.Error()
			if err != nil {
				logrus.WithError(err).Errorf("mqtt publish error mac:%v", mac)
				continue
			}
			logrus.Infof("mqtt publish done mac:%v payload:%v", mac, string(data))
		}
		logrus.Info("fetch data end")
	})
	if err != nil {
		logrus.WithError(err).Errorf("add func error")
		panic(err)
	}
	c.Start()
	logrus.Info("job start done")
	select {}
}

func accessToken(appKey, appSecret string) (string, error) {
	params := url.Values{}
	params.Set("grant_type", "client_credentials")
	params.Set("scope", "device_full_access")
	req, err := http.NewRequest("POST", "https://oauth.cleargrass.com/oauth2/token", bytes.NewBufferString(params.Encode()))
	if err != nil {
		return "", errors.Wrap(err, "new http request error")
	}
	authToken := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v:%v", appKey, appSecret)))
	req.Header.Add("Authorization", fmt.Sprintf("Basic %v", authToken))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return "", errors.Wrap(err, "http post error")
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "read response body error")
	}
	logrus.Infof("access token response:%v", string(respBody))
	var respData struct {
		AccessToken string `json:"access_token"`
	}
	err = json.Unmarshal(respBody, &respData)
	if err != nil {
		return "", errors.Wrap(err, "response body decode error")
	}
	if respData.AccessToken == "" {
		return "", errors.New("access token not found")
	}
	return respData.AccessToken, nil
}

func deviceData(accessToken string) (map[string]DeviceData, error) {
	timestamp := time.Now().UnixNano() / 1e6
	req, err := http.NewRequest("GET", fmt.Sprintf("https://apis.cleargrass.com/v1/apis/devices?timestamp=%d", timestamp), nil)
	if err != nil {
		return nil, errors.Wrap(err, "new http request error")
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %v", accessToken))
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "http get error")
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body error")
	}
	logrus.Infof("device list response:%v", string(respBody))
	var respData struct {
		Devices []struct {
			Info struct {
				Mac string `json:"mac"`
			} `json:"info"`
			Data struct {
				Timestamp struct {
					Value int64
				} `json:"timestamp"`
				Battery struct {
					Value float64 `json:"value"`
				} `json:"battery"`
				Temperature struct {
					Value float64 `json:"value"`
				} `json:"temperature"`
				Humidity struct {
					Value float64 `json:"value"`
				} `json:"humidity"`
				TVOC struct {
					Value float64 `json:"value"`
				} `json:"tvoc"`
				CO2 struct {
					Value float64 `json:"value"`
				} `json:"co2"`
				PM25 struct {
					Value float64 `json:"value"`
				} `json:"pm25"`
			} `json:"data"`
		} `json:"devices"`
	}
	err = json.Unmarshal(respBody, &respData)
	if err != nil {
		return nil, errors.Wrap(err, "response body decode error")
	}
	if len(respData.Devices) == 0 {
		return nil, errors.New("device not found")
	}
	m := make(map[string]DeviceData)
	for _, v := range respData.Devices {
		m[v.Info.Mac] = DeviceData{
			Timestamp:   v.Data.Timestamp.Value,
			Battery:     v.Data.Battery.Value,
			Temperature: v.Data.Temperature.Value,
			Humidity:    v.Data.Humidity.Value,
			TVOC:        v.Data.TVOC.Value,
			CO2:         v.Data.CO2.Value,
			PM25:        v.Data.PM25.Value,
		}
	}
	return m, nil
}

type DeviceData struct {
	Timestamp   int64   `json:"timestamp"`
	Battery     float64 `json:"battery"`
	Temperature float64 `json:"temperature"`
	Humidity    float64 `json:"humidity"`
	TVOC        float64 `json:"tvoc"`
	CO2         float64 `json:"co2"`
	PM25        float64 `json:"pm25"`
}
