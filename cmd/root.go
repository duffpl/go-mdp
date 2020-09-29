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
  "context"
  "fmt"
  "github.com/duffpl/go-mdp/processor"
  "github.com/spf13/cobra"
  "io"
  "os"
)


var cfgFile string


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
      configFilename := cmd.Flag(FlagNameConfig).Value.String()
      p, err := processor.NewProcessorWithConfigFile(configFilename)
      if err != nil {
        return fmt.Errorf("cannot create processor: %w", err)
      }
      processedLines, errCh := p.ProcessInput(input, context.Background())
      buff := newOrderedLineBuffer(output)
      for line := range processedLines {
        select {
        case err := <-errCh:
          if err != nil {
            return fmt.Errorf("cannot process: %w", err)
          }
        default:
        }
        err := buff.WriteLine(line)
        if err != nil {
          return fmt.Errorf("cannot write to buffer: %w", err)
        }
      }
      return nil
    },
}

func newOrderedLineBuffer(output io.Writer) *orderedLineBuffer {
  return &orderedLineBuffer{
    lineBuffer: make(map[int]string),
    output: output,
  }
}

type orderedLineBuffer struct {
  currentLine int
  lineBuffer map[int]string
  output io.Writer
}

func (b *orderedLineBuffer) WriteLine(line processor.OrderedLine) error {
  b.lineBuffer[line.Order] = line.Line
  return b.processBuffer()
}

func (b *orderedLineBuffer) processBuffer() error {
  if line, found := b.lineBuffer[b.currentLine]; found {
    _, err := b.output.Write([]byte(line))
    if err != nil {
      return err
    }
    delete(b.lineBuffer, b.currentLine)
    b.currentLine++
    return b.processBuffer()
  }
  return nil
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
  FlagNameInput = "input"
  FlagNameOutput = "output"
  FlagNameConfig = "config"
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

