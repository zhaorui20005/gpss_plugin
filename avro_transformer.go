package main

/**
This is a simple transformer plugin implementation, it's used mainly for e2e tests.
The transformer accepts data like {"A":2,"B":1} as input, and takes operator from properties,
if operator=="+", it puts C=A+B back.
if operator=='-', it puts C=A-B back.
otherwise, an error occurs
*/
import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	common "github.com/greenplum-db/gp-stream-server-plugin"
	"github.com/greenplum-db/gp-stream-server-plugin/transformer"
	"github.com/karrick/goavro"
)

// ErrInitMethodUnexpectedInvoked is thrown when SimpleTransformOnInit is invoked 0 or multi times
// nolint
var ErrInitMethodUnexpectedInvoked = errors.New("init method is invoked 0 or multi times")
var ErrNoSchemaFileSpecified = errors.New("no schema file or URL specified")
var ErrNeedColumnNumber = errors.New("need to set column_number when dynamic_csv is set")

// initTimes records the count which init method is invoked
// nolint
var initTimes int32 = 0

// nolint
var name string
var schemaString string
var registryURL string
var schemaValue sync.Map

var csvColumnNum int
var csvValueType map[string]string
var dynamicOrder bool

// SchemaRegistryClient defines an interface for fetching schemas from a schema registry.
type SchemaRegistryClient interface {
	GetSchemaByID(ctx context.Context, id int) (string, error)
}

// HTTPClientSchemaRegistry is a SchemaRegistryClient that uses an HTTP client.
type HTTPClientSchemaRegistry struct {
	baseURL string
	client  *http.Client
}

// NewHTTPClientSchemaRegistry creates a new HTTPClientSchemaRegistry.
func NewHTTPClientSchemaRegistry(baseURL string) *HTTPClientSchemaRegistry {
	return &HTTPClientSchemaRegistry{
		baseURL: baseURL,
		client:  &http.Client{},
	}
}

// GetSchemaByID retrieves a schema by its ID from the schema registry.
func (c *HTTPClientSchemaRegistry) GetSchemaByID(ctx context.Context, id int) (string, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}
	return string(body), nil
}

// SimpleTransformOnInit is invoked when plugin is loaded
// nolint
func SimpleTransformOnInit(ctx common.BaseContext) error {
	atomic.AddInt32(&initTimes, 1)
	properties := ctx.GetProperties()
	name = properties["name"]
	var schemaURLExist bool
	registryURL, schemaURLExist = properties["schema_url"]
	fileName, schemaFileExist := properties["schema_file"]

	if !schemaFileExist && !schemaURLExist {
		return ErrNoSchemaFileSpecified
	}
	if schemaFileExist {
		schemaStr, err := os.ReadFile(fileName)
		if err != nil {
			return err
		}
		schemaString = string(schemaStr)
	}
	dynamicStr, ok := properties["dynamic_csv"]
	if ok {
		dynamicOrder, _ = strconv.ParseBool(dynamicStr)
	}
	if dynamicOrder {
		csvValueType = make(map[string]string)
		csvColumnNumStr, ok := properties["column_number"]
		if !ok {
			return ErrNeedColumnNumber
		}
		var err error
		csvColumnNum, err = strconv.Atoi(csvColumnNumStr)
		if err != nil {
			return err
		}
		for i := 1; i <= csvColumnNum; i++ {
			valueStr := fmt.Sprintf("value_%d", i)
			valueType := fmt.Sprintf("type_%d", i)
			valueString, existance := properties[valueStr]
			if !existance {
				return fmt.Errorf("the value_%d is missing", i)
			}
			csvValueType[valueStr] = valueString
			typeString, existance := properties[valueType]
			if !existance {
				return fmt.Errorf("the value_%d is missing", i)
			}
			csvValueType[valueType] = typeString
		}
	}

	ctx.GetLogger().Infof("plugin:%s init start...", name)
	// do some init work...

	ctx.GetLogger().Infof("plugin:%s init finished", name)
	return nil

}

// SimpleTransform is invoked every message arrived
// nolint
func SimpleTransform(ctx transformer.TransformContext) {
	// whenever InitTimes!=1, report an error, this is a check to ensure SimpleTransformOnInit is invoked exactly once
	if atomic.LoadInt32(&initTimes) != 1 {
		ctx.SetTransformStatus(transformer.TransformStatusError)
		ctx.SetError(ErrInitMethodUnexpectedInvoked)
		return
	}
	logger := ctx.GetLogger()
	logger.Infof("start to transform using %s", name)

	input := ctx.GetInput()
	logger.Infof("The input bytes len(%d): %v", len(input), input)
	var schemaStr string

	if len(schemaString) == 0 {
		magicByte := input[0]
		if magicByte != 0 {
			logger.Errorf("incorrect magic byte: %d, should be 0", magicByte)
			ctx.SetTransformStatus(transformer.TransformStatusError)
			return
		}
		schemaIDBytes := input[1:5]
		schemaID := int32(schemaIDBytes[0])<<24 | int32(schemaIDBytes[1])<<16 | int32(schemaIDBytes[2])<<8 | int32(schemaIDBytes[3])
		value, ok := schemaValue.Load(schemaID)
		if ok {
			schemaStr = value.(string)
		} else {
			registryClient := NewHTTPClientSchemaRegistry(registryURL)
			schemaStr, err := registryClient.GetSchemaByID(context.Background(), int(schemaID))
			if err != nil {
				logger.Errorf("get registry schema failed: %v", err)
				ctx.SetTransformStatus(transformer.TransformStatusError)
				return
			}
			schemaValue.Store(schemaID, schemaStr)
		}

		input = input[5:]
	} else {
		schemaStr = schemaString
	}

	// Create a new codec from the schema
	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		logger.Fatal(err)
	}

	b := &bytes.Buffer{}
	wr := csv.NewWriter(b)
	wr.Comma = ','
	// Convert binary data to native Go data

	for {
		if len(input) == 0 {
			break
		}
		nativeData, left, err := codec.NativeFromBinary(input)
		if err != nil {
			logger.Infof("Stop decode in transform, err:(%v)", len(left), err)
			break
		}
		// Assert the type of the decoded data
		record, ok := nativeData.(map[string]interface{})
		if !ok {
			logger.Errorf("Expected map[string]interface{}, got %T", nativeData)
			break
		}

		lines := []string{}

		if dynamicOrder {
			err = formatJsonToLines(record, &lines)
			if err != nil {
				logger.Errorf("formatToLines failed with error:%v", err)
				break
			}
		} else {
			lines = append(lines, record["username"].(string))
			lines = append(lines, record["tweet"].(string))
			t := time.Unix(record["timestamp"].(int64), 0)

			lines = append(lines, fmt.Sprint(t.Format("2006-01-02 15:04:05")))
			lines = append(lines, string((record["photo"].([]uint8))[0]))
		}

		wr.Write(lines)
		wr.Flush()

		input = left
	}

	ctx.SetOutput(b.Bytes())
	ctx.SetTransformStatus(transformer.TransformStatusAccept)

	logger.Infof("finished to transform using %s", name)
}

// No error handling
func formatJsonToLines(record map[string]interface{}, lines *[]string) error {
	for i := 1; i <= csvColumnNum; i++ {
		valueStr := fmt.Sprintf("value_%d", i)
		valueType := fmt.Sprintf("type_%d", i)
		addColumnToLines(record, lines, valueStr, valueType)
	}
	return nil
}
func addColumnToLines(record interface{}, lines *[]string, valueStr string, typeStr string) error {
	index := strings.Index(valueStr, "/")
	if index != -1 {
		previousStr := valueStr[0:index]
		postStr := valueStr[index+1:]
		if indexNum, err := strconv.Atoi(previousStr); err == nil {
			target, _ := record.([]interface{})
			return addColumnToLines(target[indexNum], lines, postStr, typeStr)
		} else {
			target, _ := record.(map[string]interface{})
			return addColumnToLines(target[previousStr], lines, postStr, typeStr)
		}
	}
	if indexNum, err := strconv.Atoi(valueStr); err == nil {
		target, _ := record.([]interface{})

		switch typeStr {
		case "timestamp":
			t := time.Unix(target[indexNum].(int64), 0)
			*lines = append(*lines, fmt.Sprint(t.Format("2006-01-02 15:04:05")))
		default:
			*lines = append(*lines, fmt.Sprintf("%v", target[indexNum]))
		}
	} else {
		fmt.Printf("The type of record is %T:%v\n", record, record)
		target, ok := record.(map[string]interface{})
		fmt.Printf("OK is %v, The type of target is %T:%v\n", ok, target, target)

		switch typeStr {
		case "timestamp":
			t := time.Unix(target[valueStr].(int64), 0)
			*lines = append(*lines, fmt.Sprint(t.Format("2006-01-02 15:04:05")))
		default:
			*lines = append(*lines, fmt.Sprintf("%v", target[valueStr]))
		}
	}
	return nil
}
