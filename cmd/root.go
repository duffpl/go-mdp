// Copyright 2020 go-mdp contributors
// Licensed under the Apache License, Version 2.0
package cmd

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/duffpl/go-mdp/v2/config"
	"github.com/duffpl/go-mdp/v2/processor"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "go-mdp",
	Short: "MySQL dump anonymizer/processor",
	Long:  "go-mdp processes MySQL dump files and anonymizes data based on a JSON configuration.",
	RunE: func(cmd *cobra.Command, args []string) error {
		p, err := initProcessor(cmd)
		if err != nil {
			return fmt.Errorf("cannot create processor: %w", err)
		}
		return runProcessor(cmd.Context(), p, cmd.Flag(FlagNameInput).Value.String(), cmd.Flag(FlagNameOutput).Value.String())
	},
}

func initProcessor(cmd *cobra.Command) (*processor.Processor, error) {
	var configData []byte
	flagConfigData, err := cmd.Flags().GetString(FlagNameConfigData)
	if err != nil {
		return nil, fmt.Errorf("cannot get config data flag: %w", err)
	}
	if flagConfigData != "" {
		configData, err = base64.StdEncoding.DecodeString(flagConfigData)
		if err != nil {
			return nil, fmt.Errorf("cannot decode config data: %w", err)
		}
		isConfigDataZipped, err := cmd.Flags().GetBool(FlagNameConfigIsZipped)
		if err != nil {
			return nil, fmt.Errorf("cannot get is config zipped flag: %w", err)
		}
		if isConfigDataZipped {
			gr, err := gzip.NewReader(bytes.NewBuffer(configData))
			if err != nil {
				return nil, fmt.Errorf("cannot create gzip reader: %w", err)
			}
			defer gr.Close()
			configData, err = io.ReadAll(gr)
			if err != nil {
				return nil, fmt.Errorf("cannot read gzip data: %w", err)
			}
		}
	} else {
		configFilename, err := cmd.Flags().GetString(FlagNameConfig)
		if err != nil {
			return nil, fmt.Errorf("cannot read config filename flag: %w", err)
		}
		configData, err = os.ReadFile(configFilename)
		if err != nil {
			return nil, fmt.Errorf("cannot read config file: %w", err)
		}
	}
	configObject := &config.Config{}
	err = json.Unmarshal(configData, configObject)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal config json data: %w", err)
	}
	p, err := processor.NewProcessorWithConfig(*configObject)
	if err != nil {
		return nil, fmt.Errorf("cannot create processor with config json data: %w", err)
	}
	return p, nil
}

func getInputStream(filename string) (io.ReadCloser, error) {
	if filename == "" {
		return os.Stdin, nil
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	return f, nil
}

// runProcessor owns CLI streams. Close the underlying descriptors to interrupt
// blocked reads/writes on cancellation; never flush the buffer on a failed run.
func runProcessor(ctx context.Context, p *processor.Processor, inputName, outputName string) error {
	input, err := getInputStream(inputName)
	if err != nil {
		return fmt.Errorf("cannot create input stream: %w", err)
	}
	defer input.Close()
	if outputName != "" {
		inFile, ok := input.(*os.File)
		if ok {
			inInfo, inErr := inFile.Stat()
			outInfo, outErr := os.Stat(outputName)
			if inErr != nil {
				return inErr
			}
			if outErr == nil && os.SameFile(inInfo, outInfo) {
				return fmt.Errorf("input and output must be different files")
			}
		}
	}
	output, err := getOutputStream(outputName)
	if err != nil {
		return fmt.Errorf("cannot create output stream: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { input.Close(); output.Abort() })
	defer stop()
	if err := p.Process(input, output, ctx); err != nil {
		input.Close()
		output.Abort()
		return err
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("cannot finish output: %w", err)
	}
	return ctx.Err()
}

type bufferedWriteCloser struct {
	mu sync.Mutex
	w  *bufio.Writer
	f  *os.File
}

func (b *bufferedWriteCloser) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.w.Write(p)
}
func (b *bufferedWriteCloser) WriteString(s string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.w.WriteString(s)
}
func (b *bufferedWriteCloser) Abort() { _ = b.f.Close() }
func (b *bufferedWriteCloser) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	flushErr := b.w.Flush()
	return errors.Join(flushErr, b.f.Close())
}
func getOutputStream(filename string) (*bufferedWriteCloser, error) {
	f := os.Stdout
	if filename != "" {
		var err error
		f, err = os.Create(filename)
		if err != nil {
			return nil, fmt.Errorf("cannot create file: %w", err)
		}
	}
	return &bufferedWriteCloser{w: bufio.NewWriterSize(f, 64<<10), f: f}, nil
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

var (
	FlagNameInput          = "input"
	FlagNameOutput         = "output"
	FlagNameConfig         = "config"
	FlagNameConfigData     = "config-data"
	FlagNameConfigIsZipped = "config-zipped"
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.Flags().StringP(FlagNameInput, "i", "", "input file (if not set stdin is used)")
	rootCmd.Flags().StringP(FlagNameOutput, "o", "", "output file (if not set stdout is used)")
	rootCmd.Flags().StringP(FlagNameConfig, "c", "config.json", "config file")
	rootCmd.Flags().StringP(FlagNameConfigData, "f", "", "encoded config file")
	rootCmd.Flags().BoolP(FlagNameConfigIsZipped, "z", false, "is encoded config data zipped?")
}

func initConfig() {}
