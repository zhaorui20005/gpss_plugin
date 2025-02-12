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
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	common "github.com/greenplum-db/gp-stream-server-plugin"
	"github.com/greenplum-db/gp-stream-server-plugin/transformer"
	"github.com/karrick/goavro"
)

// ErrInitMethodUnexpectedInvoked is thrown when SimpleTransformOnInit is invoked 0 or multi times
// nolint
var ErrInitMethodUnexpectedInvoked = errors.New("init method is invoked 0 or multi times")
var ErrNoSchemaFileSpecified = errors.New("no schema file specified")

// initTimes records the count which init method is invoked
// nolint
var initTimes int32 = 0

// nolint
var name string
var schemaString string

// SimpleTransformOnInit is invoked when plugin is loaded
// nolint
func SimpleTransformOnInit(ctx common.BaseContext) error {
	atomic.AddInt32(&initTimes, 1)
	properties := ctx.GetProperties()
	name = properties["name"]
	file_name, ok := properties["schema_file"]
	if !ok {
		return ErrNoSchemaFileSpecified
	}
	schemaStr, err := os.ReadFile(file_name)
	if err != nil {
		return err
	}
	schemaString = string(schemaStr)
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

	// Create a new codec from the schema
	codec, err := goavro.NewCodec(schemaString)
	if err != nil {
		logger.Fatal(err)
	}

	b := &bytes.Buffer{}
	wr := csv.NewWriter(b)
	wr.Comma = ','
	// Convert binary data to native Go data

	for {
		nativeData, left, err := codec.NativeFromBinary(input)
		if err != nil {
			logger.Infof("Finished decoding, the error is %v", err)
			break
		}
		// Assert the type of the decoded data
		record, ok := nativeData.(map[string]interface{})
		if !ok {
			logger.Errorf("Expected map[string]interface{}, got %T", nativeData)
			break
		}

		lines := []string{}
		lines = append(lines, record["username"].(string))
		lines = append(lines, record["tweet"].(string))
		t := time.Unix(record["timestamp"].(int64), 0)

		lines = append(lines, fmt.Sprint(t.Format("2006-01-02 15:04:05")))
		lines = append(lines, string((record["photo"].([]uint8))[0]))

		wr.Write(lines)
		wr.Flush()

		input = left
	}

	ctx.SetOutput(b.Bytes())
	ctx.SetTransformStatus(transformer.TransformStatusAccept)

	logger.Infof("finished to transform using %s", name)
}
