# Introduction

The Greenplum Stream Server (GPSS) is an ETL (extract, transform, load) tool for [greenplum-db](https://github.com/greenplum-db/gpdb).

GPSS 1.9.0 introduces a "kernel-plugin" mode based on [go native plugin](https://pkg.go.dev/plugin) mechanism to help customer extend their custom logic when loading data. GPSS acts as the kernel,it is responsible for loading plugin and invoking plugin function at specific stage during the job lifecycle. Customers write their own plugins and build them with `go build -buildmode=plugin` to a dynamically linked file, then specify plugin config in job config file.

This repository helps you implement your own GPSS plugin. It contains some common interfaces and constants used to implement a plugin, and also some simple examples.

Currently,the following plugin types are supported:

- [Transformer](./transformer)

# Known issues and Limitations

- The plugin should be built with same go version as GPSS.
- If a dependency that your plugin uses is also used by GPSS, their versions should be same.

Use `go version -m gpss` to check the go version and dependencies version of gpss.

As the repo of https://github.com/greenplum-db/gp-stream-server-plugin has been archived, we need to make a new directory and make a new go.mod file to build the plugin.


