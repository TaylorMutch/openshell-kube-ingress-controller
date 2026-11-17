package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	openshell "github.com/ai-openshell/sdk-go/openshell/v1"
	"io"
	"k8s.io/apimachinery/pkg/util/validation"
	"net/url"
	"os"
	"regexp"
	"strings"
)

var labelPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$`)

type Config struct {
	ControllerID       string   `json:"controllerID"`
	Namespace          string   `json:"namespace"`
	Gateway            string   `json:"gateway"`
	GatewayNamespace   string   `json:"gatewayNamespace"`
	Listener           string   `json:"listener"`
	BackendCAConfigMap string   `json:"backendCAConfigMap"`
	Sources            []Source `json:"sources"`
}
type Source struct {
	ID        string `json:"id"`
	Endpoint  string `json:"endpoint"`
	Workspace string `json:"workspace"`
	CAFile    string `json:"caFile"`
	CertFile  string `json:"certFile"`
	KeyFile   string `json:"keyFile"`
	TokenFile string `json:"tokenFile"`
}

func readConfig(path string) (Config, error) {
	var config Config
	file, err := os.Open(path)
	if err != nil {
		return config, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1024*1024))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&config); err != nil {
		return config, err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return config, fmt.Errorf("configuration must contain one JSON object")
	}
	if !labelPattern.MatchString(config.ControllerID) || config.Namespace == "" || config.Gateway == "" || config.GatewayNamespace == "" || config.Listener == "" || config.BackendCAConfigMap == "" || len(config.Sources) == 0 || len(config.Sources) > 8 {
		return config, fmt.Errorf("controller identity, namespaces, Gateway/listener, backend CA ConfigMap, and 1-8 sources are required")
	}
	if len(validation.IsDNS1123Subdomain(config.BackendCAConfigMap)) != 0 {
		return config, fmt.Errorf("invalid backend CA ConfigMap name")
	}
	seen := map[string]bool{}
	for _, source := range config.Sources {
		endpoint, e := url.Parse(source.Endpoint)
		if e != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.Path != "" && endpoint.Path != "/" || endpoint.RawQuery != "" || endpoint.Fragment != "" || !labelPattern.MatchString(source.ID) || source.Workspace == "" || seen[source.ID] || (source.CertFile == "") != (source.KeyFile == "") {
			return config, fmt.Errorf("invalid or duplicate discovery source")
		}
		seen[source.ID] = true
	}
	return config, nil
}

func (source Source) client() (*openshell.Client, error) {
	var auth openshell.AuthProvider
	if source.TokenFile != "" {
		file, err := os.Open(source.TokenFile)
		if err != nil {
			return nil, err
		}
		token, err := io.ReadAll(io.LimitReader(file, 1024*1024+1))
		_ = file.Close()
		if err != nil || len(token) > 1024*1024 || len(bytes.TrimSpace(token)) == 0 {
			return nil, fmt.Errorf("unable to read bounded discovery token")
		}
		auth = openshell.StaticToken(string(bytes.TrimSpace(token)))
	}
	return openshell.NewClient(openshell.Config{Address: strings.TrimSuffix(source.Endpoint, "/"), Auth: auth, TLS: &openshell.TLSConfig{CAFile: source.CAFile, CertFile: source.CertFile, KeyFile: source.KeyFile}})
}
