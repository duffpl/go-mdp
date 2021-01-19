/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/duffpl/go-mdp/config"
	"github.com/duffpl/go-mdp/faker"
	"github.com/duffpl/go-mdp/processor"
	"github.com/duffpl/go-mdp/templates"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"io"
	"io/ioutil"
	"os"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "go-mdp",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	RunE: func(cmd *cobra.Command, args []string) error {
		input, err := getInputStream(cmd.Flag(FlagNameInput).Value.String())
		if err != nil {
			return fmt.Errorf("cannot create input stream: %w", err)
		}
		output, err := getOutputStream(cmd.Flag(FlagNameOutput).Value.String())
		if err != nil {
			return fmt.Errorf("cannot create output stream: %w", err)
		}
		p, err := initProcessor(cmd)
		if err != nil {
			return fmt.Errorf("cannot create processor: %w", err)
		}
		l := logrus.New()
		l.SetLevel(logrus.DebugLevel)
		processor.SetLogger(l)
		return p.Process(input, output, context.Background())
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
			configData, err = ioutil.ReadAll(gr)
			if err != nil {
				return nil, fmt.Errorf("cannot read gzip data: %w", err)
			}
		}
	} else {
		configFilename, err := cmd.Flags().GetString(FlagNameConfig)
		if err != nil {
			return nil, fmt.Errorf("cannot read config filename flag: %w", err)
		}
		configData, err = ioutil.ReadFile(configFilename)
		if err != nil {
			return nil, fmt.Errorf("cannot read config file: %w", err)
		}
	}
	configObject := &config.Config{}
	err = json.Unmarshal(configData, configObject)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal config json data: %w", err)
	}
	f := faker.Faker{
		Locale: configObject.Settings.Locale,
	}
	templates.RegisterTemplateFuncs(f.FuncMap())
	p, err := processor.NewProcessorWithConfig(*configObject)
	if err != nil {
		return nil, fmt.Errorf("cannot create processor with config json data: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("cannot create processor with config json data: %w", err)
	}
	//transformations.RegisterColumnTransformations()
	return p, nil
}

func getInputStream(filename string) (io.Reader, error) {
	if filename == "" {
		return os.Stdin, nil
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	return bufio.NewReader(f), nil
}

func getOutputStream(filename string) (io.Writer, error) {
	if filename == "" {
		return os.Stdout, nil
	}
	f, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot create file: %w", err)
	}
	return bufio.NewWriter(f), nil
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
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

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	//rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.go-mdp.yaml)")
	rootCmd.Flags().StringP(FlagNameInput, "i", "", "input file (if not set stdin is used)")
	rootCmd.Flags().StringP(FlagNameOutput, "o", "", "output file (if not set stdout is used)")
	rootCmd.Flags().StringP(FlagNameConfig, "c", "config.json", "config file")
	rootCmd.Flags().StringP(FlagNameConfigData, "f", "", "encoded config file")
	rootCmd.Flags().BoolP(FlagNameConfigIsZipped, "z", false, "is encoded config data zipped?")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	//if cfgFile != "" {
	//  // Use config file from the flag.
	//  viper.SetConfigFile(cfgFile)
	//} else {
	//  // Find home directory.
	//  home, err := homedir.Dir()
	//  if err != nil {
	//    fmt.Println(err)
	//    os.Exit(1)
	//  }
	//
	//  // Search config in home directory with name ".go-mdp" (without extension).
	//  viper.AddConfigPath(home)
	//  viper.SetConfigName(".go-mdp")
	//}
	//
	//viper.AutomaticEnv() // read in environment variables that match
	//
	//// If a config file is found, read it in.
	//if err := viper.ReadInConfig(); err == nil {
	//  fmt.Println("Using config file:", viper.ConfigFileUsed())
	//}
}
